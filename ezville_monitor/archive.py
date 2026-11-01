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
    # Additive migration: latest operations summary is indexed independently of batches.
    db.execute('CREATE TABLE IF NOT EXISTS operations_snapshots (node TEXT PRIMARY KEY, collected REAL NOT NULL, body TEXT NOT NULL)')
    db.commit()


def persist(db, retention_days=30, max_storage_mb=256):
    now = time.time()
    limit = max_storage_mb * 1024 * 1024
    with db:
        for batch_id, body in db.execute('SELECT id,body FROM outbox ORDER BY rowid').fetchall():
            batch = json.loads(body)
            db.execute('INSERT OR IGNORE INTO batches VALUES (?,?,?,?,?)',
                       (batch_id, batch['node'], batch['collected_at'], now, body.decode()))
            if batch.get('operations') is not None:
                db.execute('''INSERT INTO operations_snapshots(node,collected,body) VALUES (?,?,?)
                    ON CONFLICT(node) DO UPDATE SET collected=excluded.collected,body=excluded.body
                    WHERE excluded.collected >= operations_snapshots.collected''',
                    (batch['node'], batch.get('operations_collected_at') or batch['collected_at'],
                     json.dumps(batch['operations'], separators=(',', ':'))))
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
                from packet_ranges import discover_candidates
                # The HA-authenticated ingress can inspect samples that already
                # exist in the local source log. Remote reads remain fingerprints only.
                candidate_report = discover_candidates(database, node, time.time(), include_samples=True)
                candidate_lines = [
                    '후보 시각은 5분 프로파일 구간입니다. 정확한 행동 시각이나 패킷 의미를 확정하지 않습니다.',
                    '지문은 원시 바이트가 아니므로 층수 같은 숫자값을 해독할 수 없습니다.',
                    '완전한 관측 구간과 같은 시간대의 과거 기준선이 부족하면 후보 누락/오탐 가능성이 큽니다.',
                    '관측 상태: {} · 완전한 프로파일 구간 {}개 · 불완전 프로파일 구간 {}개 '
                    '· overflow 구간 {}개 · 누락 {}개 · 수집 공백 최대 {}초 · 잘림 {}'.format(
                        '완전' if candidate_report['observation_complete'] else '불완전',
                        candidate_report['complete_profile_buckets'], candidate_report['incomplete_profile_windows'],
                        candidate_report['overflowed_profile_windows'], candidate_report['missing_profile_buckets'],
                        candidate_report['collector_max_gap_seconds'],
                        '있음' if candidate_report['truncated'] else '없음')]
                candidate_lines.append('DB 행 검사 {}/{}개 · 일치 행 최소 {}개 · 오래된 생략 최소 {}개'.format(
                    candidate_report['batches_scanned'], candidate_report['batch_scan_limit'],
                    candidate_report['matching_rows_detected_at_least'],
                    candidate_report['older_rows_omitted_at_least']))
                candidate_lines.append('기준선 설정 최대 {}일, 최소 {}일 · 실제 후보별 기준선 일수는 각 항목에 따로 표시'.format(
                    candidate_report['baseline_days_configured'], candidate_report['minimum_baseline_days_required']))
                candidate_lines.append('발견 후보 {}개 · 화면에는 최대 20개 표시 · 제외 항목 {}'.format(
                    candidate_report['candidate_total'],
                    json.dumps(candidate_report['candidate_suppressed_counts'], ensure_ascii=False, sort_keys=True)))
                if candidate_report['candidate_list_truncated']:
                    candidate_lines.append('후보 응답 상한 50개에 도달해 일부 후보를 생략했습니다.')
                for candidate in candidate_report['candidates'][:20]:
                    start_text = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(candidate['bucket_start'] + 9 * 3600))
                    end_text = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(candidate['bucket_end'] + 9 * 3600))
                    candidate_lines.append(
                        '프로파일 구간 {}–{} KST · {} · 현재 {} / 앞 {} / 뒤 {} · '
                        '동일시각 실제 기준선 {}일/{}개 구간'.format(
                            start_text, end_text, candidate['signature'], candidate['count'],
                            candidate['control_before'], candidate['control_after'],
                            candidate['same_slot_baseline_days_observed'],
                            candidate['same_slot_baseline_buckets']))
                    candidate_lines.append('이 시간대에 기억나는 행동이 있었나요? 확실하지 않으면 기억나지 않는다고 답해 주세요.')
                    if candidate.get('raw_samples'):
                        candidate_lines.append('HA 내부 기존 원문 표본(최대 3개): ' + ', '.join(candidate['raw_samples']))
                if not candidate_report['candidates']:
                    candidate_lines.append('현재 기준으로 표시할 수 있는 패턴 후보가 없습니다.')
                candidate_text = '\n'.join(candidate_lines)
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
                    + html.escape(daily_text) + '</pre><h2>미해석 패킷 후보 · 최근 24시간</h2><pre>'
                    + html.escape(candidate_text) + '</pre><h2>최근 분석</h2><pre>'
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
