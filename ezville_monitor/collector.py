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


def collect(db, log_path, node, now, max_batches=1000, status=None):
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
                 'collected_at': now, 'events': events}
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


def main():
    from archive import initialize, persist, serve
    options = json.loads(Path('/data/options.json').read_text())
    database = '/data/telemetry.sqlite'
    db = connect(database)
    initialize(db)
    from maintenance import initialize as initialize_weekly
    initialize_weekly(db)
    server = serve(database, options['node_id'])
    remote_server = None
    if options.get('remote_enabled'):
        from remote_api import serve as serve_remote
        try:
            remote_server = serve_remote(database, options['node_id'], options.get('remote_secret', ''))
            print('remote API active on internal port 8788', flush=True)
        except (ValueError, OSError) as error:
            # Fail closed for remote reads while preserving local collection.
            print('remote API not started: ' + type(error).__name__ + '; check remote_secret/listener configuration', flush=True)
    stop = threading.Event()
    from maintenance import worker
    threading.Thread(target=worker, args=(database, options, stop), daemon=True).start()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.is_set():
        full = False
        try:
            status = supervisor_status(options['target_slug'], os.environ['SUPERVISOR_TOKEN'])
            batch = collect(db, options['log_path'], options['node_id'], time.time(), status=status)
            persist(db, options['retention_days'], options['max_storage_mb'])
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
