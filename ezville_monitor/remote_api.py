"""Opt-in, read-only API for a TLS tunnel; independent of HA browser authentication."""
from collections import deque
from contextlib import closing
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import sqlite3
import threading
import time
from archive import snapshot
from protocol import redact

MAX_REQUEST = 1024
MAX_RESPONSE = 262144
PATHS = {'/v1/health', '/v1/report', '/v1/recent', '/v1/daily'}


def daily_snapshot(database, node, options, now):
    """Read daily delivery state and passive discovery evidence, without raw frames."""
    from daily import window, summarize
    day, start, end = window(now)
    with closing(sqlite3.connect('file:' + str(database) + '?mode=ro', uri=True)) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        state = dict(db.execute('SELECT key,value FROM daily_state')) if 'daily_state' in tables else {}
        row = db.execute('SELECT day,status,attempted,body FROM daily_reports ORDER BY day DESC LIMIT 1').fetchone() if 'daily_reports' in tables else None
    saved = bool(row and row[0] == day and row[1] == 'done')
    summary = json.loads(row[3]) if saved else summarize(database, node, start, end)
    totals = {'packet_patterns': len(summary['packet_patterns']),
              'packet_markers': len(summary['packet_markers']), 'calls': len(summary['calls'])}
    new = set(summary['new_packet_patterns'])
    summary['packet_patterns'] = sorted(summary['packet_patterns'], key=lambda item:
        (item['signature'] not in new, item['known'], -item['count']))[:128]
    summary['new_packet_patterns'] = [item['signature'] for item in summary['packet_patterns'] if item['signature'] in new]
    summary['packet_markers'] = summary['packet_markers'][-20:]
    summary['calls'] = summary['calls'][-30:]
    # Raw bus addresses/payloads stay inside HA. Fingerprints and lengths let a
    # reviewer compare variants without treating log content as instructions.
    for item in summary['packet_patterns']:
        samples = item.pop('samples')
        item['sample_fingerprints'] = [hashlib.sha256(sample.encode()).hexdigest() for sample in samples]
        item['sample_byte_lengths'] = [len(sample) // 2 for sample in samples
                                      if re.fullmatch(r'(?:[0-9A-F]{2})+', sample)]
    for marker in summary['packet_markers']:
        marker['label'] = redact(str(marker['label']))[:80]
        for candidate in marker['candidates']:
            candidate.pop('sample', None)
    for call in summary['calls']:
        call.pop('request_id', None)
    return {'node': node, 'monitor_version': '0.5.0', 'window_day': day,
            'daily_enabled': bool(options.get('daily_enabled')) if options is not None else None,
            'mobile_notification_configured': bool(options.get('daily_notify_service')) if options is not None else None,
            'enabled_since': state.get('enabled_since'),
            'latest_delivery': None if row is None else {'day': row[0], 'status': row[1], 'attempted_at': row[2]},
            'delivery_due_at': end + 90, 'summary_source': 'saved_report' if saved else 'preview',
            'summary': summary, 'totals_before_limits': totals,
            'truncated': any(totals[key] > len(summary[key]) for key in totals),
            'limitations': ['Unknown signatures and marker correlations are hypotheses, not verified device meanings.',
                            'Raw packet samples remain in HA; fingerprints cannot reconstruct payloads.',
                            'Delivery status done means configured HA API calls returned successfully, not that a phone displayed the push.']}


def sign(secret, method, path, timestamp, nonce, body):
    envelope = '\n'.join(('ezville-read-v1', method, path, timestamp, nonce)).encode() + b'\n' + body
    return hmac.new(secret.encode(), envelope, hashlib.sha256).hexdigest()


class Guard:
    def __init__(self):
        self.lock = threading.Lock()
        self.nonces = {}
        self.requests = deque()

    def allow(self, now):
        with self.lock:
            while self.requests and self.requests[0] < now - 60:
                self.requests.popleft()
            if len(self.requests) >= 60:
                return False
            self.requests.append(now)
            return True

    def authenticate(self, secret, path, stamp, nonce, body, supplied, now):
        if not isinstance(stamp, str) or not re.fullmatch(r'\d{10}', stamp) or abs(now-int(stamp)) > 60:
            return False
        if not isinstance(nonce, str) or not re.fullmatch(r'[a-f0-9]{32}', nonce):
            return False
        if not hmac.compare_digest(sign(secret, 'POST', path, stamp, nonce, body), supplied):
            return False
        with self.lock:
            self.nonces = {key: expiry for key, expiry in self.nonces.items() if expiry >= now}
            if nonce in self.nonces:
                return False
            self.nonces[nonce] = now + 120
        return True


def recent(database, node, limit=20):
    # Bound work even with a large archive. A page can be shorter than requested
    # if the last 100 batches contain only collector heartbeats.
    events = []
    with closing(sqlite3.connect(database)) as db:
        rows = db.execute('SELECT collected,body FROM batches WHERE node=? ORDER BY collected DESC LIMIT 100', (node,))
        for collected, body in rows:
            for event in reversed(json.loads(body)['events']):
                if event['kind'] == 'log':
                    if any(tag in event['text'] for tag in ('[PACKET_WINDOW]', '[PACKET_PROFILE]', '[PACKET_TRACE]', '[PACKET_MARK]', '[PACKET_OVERFLOW]', '[SIGNAL]')):
                        continue  # Raw bus frames stay inside HA, even for an authenticated remote reader.
                    events.append({'collected_at': collected, 'source': event['source'], 'text': redact(event['text']).encode()[:4096].decode('utf-8',errors='ignore')})
                    if len(events) >= limit:
                        return events
    return events


class LimitedServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, *args, **kwargs):
        self.slots = threading.BoundedSemaphore(8)
        super().__init__(*args, **kwargs)

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


def handler(database, node, secret, options=None):
    guard = Guard()
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def log_message(self, *_):
            pass

        def reply(self, status, data, nonce=''):
            body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
            if len(body) > MAX_RESPONSE:
                status, body = 503, b'{}'
            stamp = str(int(time.time()))
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('X-Ezville-Time', stamp)
            self.send_header('X-Ezville-Signature', sign(secret, 'RESPONSE:' + str(status), self.path, stamp, nonce, body))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self.reply(404, {})

        def do_POST(self):
            if self.path not in PATHS:
                return self.reply(404, {})
            now = time.time()
            if not guard.allow(now):
                return self.reply(429, {})
            try:
                if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) != 1:
                    return self.reply(400, {})
                size = int(self.headers['Content-Length'])
                if not 0 < size <= MAX_REQUEST:
                    return self.reply(413, {})
                body = self.rfile.read(size)
                nonce = self.headers.get('X-Ezville-Nonce', '')
                if len(body) != size or not guard.authenticate(secret, self.path,
                    self.headers.get('X-Ezville-Time'), nonce, body,
                    self.headers.get('X-Ezville-Signature', ''), now):
                    return self.reply(401, {})
                payload = json.loads(body)
                if not isinstance(payload, dict) or set(payload) - ({'limit'} if self.path == '/v1/recent' else set()):
                    return self.reply(400, {}, nonce)
                if self.path == '/v1/recent':
                    limit = payload.get('limit',20)
                    if type(limit) is not int or not 1 <= limit <= 30:
                        return self.reply(400, {}, nonce)
                    result = {'node': node, 'events': recent(database, node, limit)}
                elif self.path == '/v1/daily':
                    result = daily_snapshot(database, node, options, now)
                else:
                    result = snapshot(database, node, now)
                    if self.path == '/v1/health':
                        result = {key: result[key] for key in ('node', 'window_end','batch_count', 'log_line_count',
                            'latest_log_collected_at', 'collector_age_seconds', 'collector_receiving', 'latest_status', 'latest_source_exists')}
                    else:
                        result['examples'] = [redact(item).encode()[:4096].decode('utf-8',errors='ignore') for item in result['examples']]
                return self.reply(200, result, nonce)
            except (ValueError, TypeError, KeyError):
                return self.reply(400, {})
            except (OSError, sqlite3.Error):
                return self.reply(503, {})
    return Handler


def serve(database, node, secret, options=None):
    if len(secret) < 32:
        raise ValueError('remote_secret must contain at least 32 characters')
    server = LimitedServer(('0.0.0.0', 8788), handler(database, node, secret, options))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
