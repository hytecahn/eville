"""Persist cursors and telemetry in HA-local storage."""
import glob
import json
import os
from pathlib import Path
import signal
import sqlite3
import threading
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


def operations_packet_status(operations, slug, now=None):
    """Adapt the worker cache to the legacy packet report version fields."""
    now = time.time() if now is None else now
    summary = operations.get('summary') if isinstance(operations, dict) else None
    sections = summary.get('sections', {}) if isinstance(summary, dict) else {}
    core = summary.get('core', {}) if isinstance(summary, dict) else {}
    from ha_operations import SAFE_ADDON_STATES, safe_number, safe_version
    raw_last_success = operations.get('last_success_at') if isinstance(operations, dict) else None
    last_success = safe_number(raw_last_success, 0, 1e12)
    age = None
    fresh = False
    if last_success is not None:
        age = now - last_success
        fresh = 0 <= age <= 1800
    last_error = operations.get('last_error') if isinstance(operations, dict) else None
    fresh = fresh and last_error in (None, 'partial_observation')
    core_observation = sections.get('core_info', 'unavailable')
    if isinstance(core_observation, dict):
        core_observation = core_observation.get('observation', 'unavailable')
    addon = next((item for item in summary.get('addons', {}).get('addons', [])
                  if item.get('slug') == slug), None) if isinstance(summary, dict) else None
    addon_info = addon.get('info', {}) if isinstance(addon, dict) else {'observation': 'unsupported'}
    core_version = safe_version(core.get('version')) if isinstance(core, dict) else None
    addon_version = safe_version(addon_info.get('version'))
    core_observation = 'available' if fresh and core_observation == 'available' and core_version else 'unavailable'
    addon_state = addon_info.get('state')
    addon_observation = 'available' if (fresh and addon_info.get('observation') == 'available'
                                        and isinstance(addon_state, str) and addon_state in SAFE_ADDON_STATES
                                        and addon_version) else 'unavailable'
    result = {'observed_at': last_success if fresh else None, 'data_age_seconds': age,
              'last_error': last_error if isinstance(last_error, str) and last_error in {'collector_error', 'all_unavailable', 'partial_observation'} else None,
              'observation': 'available' if core_observation == addon_observation == 'available' else 'unavailable'}
    return {
        **result,
        'core': {'observation': core_observation, 'observed_at': last_success if core_observation == 'available' else None,
                 **({'version': core_version} if core_observation == 'available' else {})},
        'addon': {'observation': addon_observation, 'observed_at': last_success if addon_observation == 'available' else None,
                  **({'state': addon_info['state'], 'version': addon_version} if addon_observation == 'available' else {})},
    }


def poll_operations_if_enabled(options, token, executor=None, target_slug=''):
    """Start local HA-wide reads only after an explicit literal-true opt-in."""
    from ha_operations import operations_options, poll_operations
    if not operations_options(options)['collection']:
        return None
    return poll_operations(token, executor=executor, target_slug=target_slug)


def poll_monitor_status(options, token, executor=None):
    """Keep the original narrow Core/add-on health probe when wide collection is off."""
    target_slug = options.get('target_slug', '') if isinstance(options, dict) else ''
    operations = poll_operations_if_enabled(options, token, executor=executor,
                                            target_slug=target_slug)
    if operations is None:
        return supervisor_status(target_slug, token), None
    return operations_packet_status(operations, target_slug), operations


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
    if options.get('remote_enabled') is True:
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
    from ha_operations import cached_status, restore_cache
    restore_cache(db, options['node_id'])
    last_operations_persisted_revision = cached_status().get('revision', 0)
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.is_set():
        full = False
        try:
            # HA operations calls run in a separate, bounded worker/cache so their
            # timeouts cannot delay packet-log cursor advancement.
            status, operations = poll_monitor_status(options, os.environ['SUPERVISOR_TOKEN'])
            if operations is None:
                operations_to_persist = None
            else:
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
    db.close()


if __name__ == '__main__':
    main()
