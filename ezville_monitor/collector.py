"""Persist cursors and telemetry in HA-local storage."""
import glob
import json
import os
from pathlib import Path
import signal
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import urllib.request
import uuid
from protocol import encode, redact, signature


def connect(path):
    db = sqlite3.connect(path)
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''CREATE TABLE IF NOT EXISTS cursors (identity TEXT PRIMARY KEY, offset INTEGER);
        CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, body BLOB);''')
    return db


def collect(db, log_path, node, now, max_batches=1000, status=None, operations=None):
    # Backpressure: never silently discard unacknowledged telemetry.
    if db.execute('SELECT count(*) FROM outbox').fetchone()[0] >= max_batches:
        raise RuntimeError('outbox full: collection paused; source retention now limits recovery')
    events = []
    with db:
        paths = [Path(p) for p in glob.glob(log_path + '*')
                 if '.stacks.log' not in p and Path(p).is_file()]
        paths.sort(key=lambda p: (p.stat().st_mtime_ns, str(p)))
        for path in paths:
            with path.open('rb') as stream:
                stat = os.fstat(stream.fileno())
                identity = '{}:{}'.format(stat.st_dev, stat.st_ino)
                row = db.execute('SELECT offset FROM cursors WHERE identity=?', (identity,)).fetchone()
                offset = row[0] if row else 0
                if stat.st_size < offset:
                    events.append({'kind': 'gap', 'text': 'source log truncated', 'at': now})
                    offset = 0
                stream.seek(offset)
                while len(events) < 32:
                    line = stream.readline(65537)
                    if not line:
                        break
                    if not line.endswith(b'\n') and len(line) <= 65536:
                        break  # An incomplete line is retried at the same cursor.
                    offset = stream.tell()
                    events.append({'kind': 'log', 'source': path.name,
                                   'text': redact(line.decode('utf-8', errors='replace')), 'at': now})
                db.execute('INSERT OR REPLACE INTO cursors VALUES (?,?)', (identity, offset))
            if len(events) >= 32:
                break
        batch = {'schema': 1, 'id': str(uuid.uuid4()), 'node': node,
                 'collected_at': now, 'events': events, 'operations': operations,
                 'operations_collected_at': (operations or {}).get('last_attempt_at')}
        # Heartbeat denotes collector health only, never successful device operation.
        batch['events'].append({'kind': 'collector_heartbeat', 'at': now,
                                'source_exists': Path(log_path).is_file(),
                                'status': status or {'observation': 'unknown'}})
        db.execute('INSERT INTO outbox VALUES (?,?)', (batch['id'], encode(batch)))
    return batch


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def supervisor_status(slug, token):
    result = {}
    for name, endpoint in [('core', '/core/info'), ('addon', '/addons/' + slug + '/info')]:
        try:
            request = urllib.request.Request('http://supervisor' + endpoint,
                                            headers={'Authorization': 'Bearer ' + token})
            with urllib.request.urlopen(request, timeout=5) as response:
                data = json.loads(response.read(65536))['data']
            result[name] = {key: data[key] for key in ('state', 'version') if key in data}
            result[name]['observation'] = 'available'
        except Exception:
            result[name] = {'observation': 'unavailable'}
    return result


def operations_packet_status(operations, slug):
    """Adapt the worker cache to the legacy packet report version fields."""
    summary = operations.get('summary') if isinstance(operations, dict) else None
    sections = summary.get('sections', {}) if isinstance(summary, dict) else {}
    core = summary.get('core', {}) if isinstance(summary, dict) else {}
    addon = next((item for item in summary.get('addons', {}).get('addons', [])
                  if item.get('slug') == slug), None) if isinstance(summary, dict) else None
    addon_info = addon.get('info', {}) if isinstance(addon, dict) else {'observation': 'unsupported'}
    return {
        'core': {'observation': sections.get('core_info', 'unavailable'),
                 **({'version': core['version']} if core.get('version') is not None else {})},
        'addon': {'observation': addon_info.get('observation', 'unavailable'),
                  **{key: addon_info[key] for key in ('state', 'version') if key in addon_info}},
    }



def main():
    from archive import initialize, persist, serve
    options = json.loads(Path('/data/options.json').read_text())
    database = '/data/telemetry.sqlite'
    db = connect(database)
    initialize(db)
    from maintenance import initialize as initialize_weekly
    initialize_weekly(db)
    if not options.get('daily_enabled'):
        from daily import initialize as initialize_daily
        initialize_daily(db)
        db.execute("DELETE FROM daily_state WHERE key='enabled_since'")
        db.commit()
    server = serve(database, options['node_id'], options)
    remote_server = None
    if options.get('remote_enabled'):
        from remote_api import serve as serve_remote
        try:
            remote_server = serve_remote(database, options['node_id'], options.get('remote_secret', ''), options)
            print('remote API active on internal port 8788', flush=True)
        except (ValueError, OSError) as error:
            # Fail closed for remote reads while preserving local collection.
            print('remote API not started: ' + type(error).__name__ + '; check remote_secret/listener configuration', flush=True)
    stop = threading.Event()
    from maintenance import worker
    threading.Thread(target=worker, args=(database, options, stop), daemon=True).start()
    from ha_operations import cached_status, poll_operations, restore_cache
    restore_cache(db, options['node_id'])
    last_operations_persisted_revision = cached_status().get('revision', 0)
    operations_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='ha-operations')
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.is_set():
        full = False
        try:
            # HA operations calls run in a separate worker/cache; packet cursor advancement
            # submits work and never waits for the HA request to finish.
            operations = poll_operations(os.environ['SUPERVISOR_TOKEN'], executor=operations_executor,
                                         target_slug=options.get('target_slug', ''))
            status = operations_packet_status(operations, options.get('target_slug', ''))
            operations_to_persist = operations if operations.get('revision', 0) > last_operations_persisted_revision else None
            batch = collect(db, options['log_path'], options['node_id'], time.time(), status=status,
                            operations=operations_to_persist)
            persist(db, options['retention_days'], options['max_storage_mb'])
            if operations_to_persist is not None:
                last_operations_persisted_revision = operations_to_persist.get('revision', last_operations_persisted_revision)
            full = len(batch['events']) >= 33
        except Exception as error:
            print('collection: {}'.format(type(error).__name__), flush=True)
        stop.wait(1 if full else options['poll_seconds'])
    if remote_server:
        remote_server.shutdown()
        remote_server.server_close()
    server.shutdown()
    server.server_close()
    operations_executor.shutdown(wait=False, cancel_futures=True)
    db.close()


if __name__ == '__main__':
    main()
