"""HA-local archive and fixed-purpose authenticated query API."""
from contextlib import closing
import hmac
import json
import os
from pathlib import Path
import secrets
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs
from protocol import MAX_BODY, authenticated, signature
from reports import report


def initialize(db):
    db.execute('CREATE TABLE IF NOT EXISTS batches (id TEXT PRIMARY KEY, node TEXT, collected REAL, received REAL, body TEXT)')
    db.execute('CREATE INDEX IF NOT EXISTS batches_window ON batches(node,collected)')
    db.commit()


def persist(db, retention_days=30, max_storage_mb=256):
    now = time.time()
    limit = max_storage_mb * 1024 * 1024
    with db:
        for batch_id, body in db.execute('SELECT id,body FROM outbox ORDER BY rowid').fetchall():
            batch = json.loads(body)
            db.execute('INSERT OR IGNORE INTO batches VALUES (?,?,?,?,?)',
                       (batch_id, batch['node'], batch['collected_at'], now, body.decode()))
            db.execute('DELETE FROM outbox WHERE id=?', (batch_id,))
        db.execute('DELETE FROM batches WHERE collected < ?', (now - retention_days * 86400,))
        # A logical byte budget; SQLite reuses freed pages. WAL/filesystem overhead is extra.
        total = db.execute('SELECT coalesce(sum(length(cast(body AS BLOB))),0) FROM batches').fetchone()[0]
        while total > limit:
            rows = db.execute('SELECT id,length(cast(body AS BLOB)) FROM batches ORDER BY collected LIMIT 100').fetchall()
            if not rows:
                break
            for batch_id, size in rows:
                db.execute('DELETE FROM batches WHERE id=?', (batch_id,))
                total -= size
                if total <= limit:
                    break
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='weekly_runs'").fetchone():
            db.execute('DELETE FROM weekly_runs WHERE attempted < ?', (now - 180 * 86400,))
    db.execute('PRAGMA wal_checkpoint(PASSIVE)')


def snapshot(database, node, now, days=7):
    result = report(database, node, now, days)
    # Staleness is based on collection time, even if old data is still readable.
    with closing(sqlite3.connect(database)) as db:
        row = db.execute('SELECT max(collected) FROM batches WHERE node=?', (node,)).fetchone()
    age = None if row[0] is None else max(0, now-row[0])
    result['collector_age_seconds'] = age
    result['collector_receiving'] = age is not None and age <= 180
    return result


def handler(database, node, options=None):
    csrf_token = secrets.token_urlsafe(32)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def allowed(self):
            return self.client_address[0] == '172.30.32.2'

        def do_POST(self):
            if not self.allowed():
                return self.send_error(403)
            if self.path != '/daily/run' or not options or not options.get('daily_enabled'):
                return self.send_error(404)
            if self.headers.get('Content-Type') != 'application/x-www-form-urlencoded':
                return self.send_error(415)
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= 256:
                    return self.send_error(413)
                data = parse_qs(self.rfile.read(size).decode('ascii'), strict_parsing=True)
                if set(data) != {'csrf'} or len(data['csrf']) != 1 or not hmac.compare_digest(data['csrf'][0], csrf_token):
                    return self.send_error(403)
                from daily import run_daily
                ran = run_daily(database, options, time.time(), os.environ['SUPERVISOR_TOKEN'], force=True)
                return self.page('일일 보고를 다시 생성하고 HA 알림을 요청했습니다.' if ran else
                                 '실행되지 않았습니다. 설정, 실행 중 상태, 60초 중복 제한 또는 22:01:30 이후인지 확인하세요.')
            except (ValueError, UnicodeError, OSError, sqlite3.Error):
                return self.send_error(503)

        def do_GET(self):
            if self.path not in ('/', ''):
                return self.send_error(404)
            return self.page()

        def page(self, message=''):
            import html
            # Supervisor is the sole ingress peer. No host port is published.
            if not self.allowed():
                self.send_error(403)
                return
            try:
                result = snapshot(database, node, time.time())
                with closing(sqlite3.connect(database)) as db:
                    initialize(db)
                    from maintenance import initialize as initialize_weekly
                    from daily import initialize as initialize_daily, render as render_daily
                    initialize_weekly(db)
                    initialize_daily(db)
                    row = db.execute('SELECT week,status,body FROM weekly_runs ORDER BY week DESC LIMIT 1').fetchone()
                    day_row = db.execute('SELECT day,status,body FROM daily_reports ORDER BY day DESC LIMIT 1').fetchone()
                latest = 'No weekly analysis yet.' if row is None else row[0] + ' (' + row[1] + ')\n' + json.dumps(json.loads(row[2]), ensure_ascii=False, indent=2)
                if day_row is None:
                    daily_text = 'No daily report yet.'
                elif day_row[1] == 'done':
                    daily_text = day_row[0] + '\n' + render_daily(json.loads(day_row[2]), max_calls=None, raw_samples=True)
                else:
                    daily_text = day_row[0] + ' (' + day_row[1] + ')'
                body = ('<!doctype html><html lang="ko"><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
                    '<title>EzVille Monitor</title><style>body{font:16px system-ui;max-width:1000px;margin:30px auto;padding:20px;background:#101820;color:#eee}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#1e2b36;padding:20px;border-radius:12px}</style>'
                    '<h1>EzVille Monitor</h1><p>HA 내부 로그 보관 · 매주 월요일 08:00 한국 시간</p>'
                    '<p>수집 heartbeat는 실제 엘리베이터 도착을 보장하지 않습니다.</p>'
                    + ('<p role="status">' + html.escape(message) + '</p>' if message else '')
                    + ('<form method="post" action="daily/run"><input type="hidden" name="csrf" value="'
                       + html.escape(csrf_token, quote=True) + '"><button type="submit">마지막 22:00 일일 보고 다시 실행 · 알림 재전송</button></form>'
                       '<p>22:01:30 이후에 사용하세요. 버튼을 다시 누르면 휴대폰 알림도 재전송될 수 있습니다.</p>'
                       if options and options.get('daily_enabled') else '')
                    + '<h2>주간 수집 현황</h2><pre>'
                    + html.escape(json.dumps(result,ensure_ascii=False,indent=2)) + '</pre><h2>일일 보고</h2><pre>'
                    + html.escape(daily_text) + '</pre><h2>최근 분석</h2><pre>'
                    + html.escape(latest) + '</pre></html>').encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Cache-Control', 'no-store')
                self.send_header('X-Content-Type-Options', 'nosniff')
                self.send_header('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'self'")
                self.end_headers()
                self.wfile.write(body)
            except (OSError, sqlite3.Error):
                self.send_error(503)
    return Handler


def serve(database, node, options=None):
    server = ThreadingHTTPServer(('0.0.0.0', 8787), handler(database, node, options))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
