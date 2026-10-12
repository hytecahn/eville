"""Run behind a TLS reverse proxy. No public read or control API."""
import json
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ezville_monitor'))
from protocol import MAX_BODY, authenticated


def connect(path):
    db = sqlite3.connect(path, timeout=10)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE IF NOT EXISTS batches (id TEXT PRIMARY KEY, node TEXT, collected REAL, received REAL, body TEXT)')
    return db


def accept(db, body, now):
    batch = json.loads(body)
    if batch.get('schema') != 1 or not isinstance(batch.get('events'), list):
        raise ValueError('invalid schema')
    import uuid
    uuid.UUID(batch['id'])
    if not isinstance(batch['node'], str) or not 1 <= len(batch['node']) <= 64:
        raise ValueError('invalid node')
    collected = float(batch['collected_at'])
    if not 0 < collected <= now + 300 or len(batch['events']) > 33:
        raise ValueError('invalid batch timestamp or size')
    for event in batch['events']:
        if event.get('kind') not in ('log', 'gap', 'collector_heartbeat'):
            raise ValueError('invalid event kind')
        if not isinstance(event.get('at'), (int, float)) or event['at'] != collected:
            raise ValueError('invalid event timestamp')
        if event['kind'] in ('log', 'gap') and not isinstance(event.get('text'), str):
            raise ValueError('invalid event text')
        if len(event.get('text', '')) > 4096:
            raise ValueError('event text too large')
    with db:
        previous = db.execute('SELECT body FROM batches WHERE id=?', (batch['id'],)).fetchone()
        if previous and previous[0] != body.decode():
            raise ValueError('batch ID collision')
        db.execute('INSERT OR IGNORE INTO batches VALUES (?,?,?,?,?)',
                   (batch['id'], batch['node'], collected, now, body.decode()))
        db.execute('DELETE FROM batches WHERE received < ?', (now - 30 * 86400,))
    return batch['id']


def handler(database, secret):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, code, data):
            encoded = json.dumps(data).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_POST(self):
            if self.path != '/ingest':
                return self.reply(404, {})
            self.connection.settimeout(10)
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= MAX_BODY or self.headers.get('Transfer-Encoding'):
                    return self.reply(413, {})
                body = self.rfile.read(size)
                now = time.time()
                if not authenticated(secret, self.headers.get('X-Ezville-Time'), body,
                                     self.headers.get('X-Ezville-Signature', ''), now):
                    return self.reply(401, {})
                with closing(connect(database)) as db:
                    accepted = accept(db, body, now)
                return self.reply(200, {'accepted': accepted})
            except (ValueError, KeyError, TypeError):
                return self.reply(400, {})
            except (OSError, sqlite3.Error):
                return self.reply(503, {})
    return Handler


if __name__ == '__main__':
    secret = os.environ['EZVILLE_SHARED_SECRET']
    if len(secret) < 32:
        raise ValueError('shared secret too short')
    ThreadingHTTPServer(('127.0.0.1', int(os.environ.get('PORT', '8787'))),
                        handler(os.environ['EZVILLE_DB'], secret)).serve_forever()
