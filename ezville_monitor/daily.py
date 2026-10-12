"""Deterministic, HA-local daily EW11 report. Log text is never forwarded to HA notifications."""
from collections import Counter, deque
import datetime as dt
import json
import re
import sqlite3
import urllib.request

KST = dt.timezone(dt.timedelta(hours=9))
CALL = re.compile(r'\[EVENT\] elevator_call_sent direction=(up|down) request_id=([a-f0-9]{32})')
ARRIVAL = '[EVENT] elevator_arrival_signal'
TS_MS = re.compile(r'\[ts_ms=(\d{13})\]')
EPOCH_S = re.compile(r'\[epoch_s=(\d{10}(?:\.\d+)?)\]')
NOTIFY = re.compile(r'^notify\.mobile_app_[a-z0-9_]+$')
MATCH_SECONDS = 900
PROFILE = '[PACKET_PROFILE] '
WINDOW = '[PACKET_WINDOW] '
MARK = '[PACKET_MARK] '
TRACE = '[PACKET_TRACE] '
DEVICE_IDS = {'0E': '조명', '36': '온도조절', '50': '콘센트', '12': '가스', '33': '일괄/엘리베이터'}
STATE_CHANGE = re.compile(r'\[EVENT\] device_state_changed device=(light|thermostat) address=(\d{2}_\d{2}) field=([A-Za-z]+) from=([^ ]+) to=([^ ]+)')


def window(now):
    local = dt.datetime.fromtimestamp(now, KST)
    end = dt.datetime.combine(local.date(), dt.time(22), KST)
    if local < end:
        end -= dt.timedelta(days=1)
    return end.date().isoformat(), (end - dt.timedelta(days=1)).timestamp(), end.timestamp()


def event_time(text):
    match = TS_MS.search(text)
    if match:
        return int(match[1]) / 1000
    match = EPOCH_S.search(text)
    return float(match[1]) if match else None


def summarize(database, node, start, end):
    counts = Counter()
    calls, arrivals, beats, gaps = [], [], [], 0
    patterns, markers = {}, {}
    packet_windows = []
    pattern_overflow = 0
    latest_source_exists = None
    logs_without_time = 0
    with sqlite3.connect('file:' + str(database) + '?mode=ro', uri=True) as db:
        has_baseline = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='packet_patterns'").fetchone()
        seen = {row[0] for row in db.execute('SELECT signature FROM packet_patterns')} if has_baseline else set()
        rows = db.execute('SELECT collected,body FROM batches WHERE node=? AND collected>=? AND collected<=? ORDER BY collected',
                          (node, start, end + 90))
        for collected, body in rows:
            for event in json.loads(body)['events']:
                if event['kind'] == 'collector_heartbeat':
                    if collected >= end:
                        continue
                    beats.append(collected)
                    latest_source_exists = bool(event.get('source_exists'))
                    if not latest_source_exists:
                        gaps += 1
                    continue
                if event['kind'] == 'gap':
                    if collected < end:
                        gaps += 1
                    continue
                if event['kind'] != 'log':
                    continue
                line = event['text']
                if WINDOW in line:
                    try:
                        item = json.loads(line.split(WINDOW, 1)[1])
                        if start < item['end'] and item['start'] < end:
                            packet_windows.append(min(end, item['end']))
                    except (ValueError, TypeError, KeyError):
                        pattern_overflow += 1
                    continue
                if PROFILE in line:
                    try:
                        item = json.loads(line.split(PROFILE, 1)[1])
                        key = item['signature']
                        if (not re.fullmatch(r'[0-9A-F]{2}(?::[0-9A-F]{2}){3}', key)
                                or not start <= item['first'] <= item['last'] < end):
                            continue
                        count = int(item['count'])
                        if count < 0:
                            raise ValueError('negative profile count')
                        entry = patterns.setdefault(key, {'signature': key, 'known': bool(item['known']),
                            'count': 0, 'samples': [], 'first': item['first'], 'last': item['last'],
                            'hourly_counts': [0] * 24, 'active_buckets': 0,
                            'peak_5min_count': 0, 'peak_5min_at': None,
                            'tracked_variants': {}, 'other_variant_count': 0, 'variant_changes': 0})
                        entry['count'] += count
                        entry['last'] = max(entry['last'], item['last'])
                        # A profile covers one five-minute bucket. Older producers lack
                        # bucket_start, so their first observed frame supplies the hour.
                        bucket = item.get('bucket_start', item['first'])
                        if not start <= bucket < end:
                            bucket = item['first']
                        hour = dt.datetime.fromtimestamp(bucket, KST).hour
                        entry['hourly_counts'][hour] += count
                        entry['active_buckets'] += 1
                        if count > entry['peak_5min_count']:
                            entry['peak_5min_count'] = count
                            entry['peak_5min_at'] = bucket
                        variants = item.get('variants', {})
                        if not isinstance(variants, dict) or len(variants) > 8:
                            raise ValueError('invalid variants')
                        for fingerprint, amount in variants.items():
                            amount = int(amount)
                            if not re.fullmatch(r'[0-9a-f]{32}', fingerprint) or amount < 0:
                                raise ValueError('invalid variant')
                            tracked = entry['tracked_variants']
                            if fingerprint in tracked:
                                tracked[fingerprint] += amount
                            elif len(tracked) < 8:
                                tracked[fingerprint] = amount
                            else:
                                entry['other_variant_count'] += amount
                        other = int(item.get('other_variant_count', 0))
                        changes = int(item.get('variant_changes', 0))
                        if other < 0 or changes < 0 or changes >= count and count > 0:
                            raise ValueError('invalid variant totals')
                        entry['other_variant_count'] += other
                        entry['variant_changes'] += changes
                        for sample in item['samples']:
                            if sample not in entry['samples'] and len(entry['samples']) < 3:
                                entry['samples'].append(sample[:256])
                    except (ValueError, TypeError, KeyError, IndexError):
                        pattern_overflow += 1
                    continue
                if '[PACKET_OVERFLOW]' in line:
                    try:
                        pattern_overflow += int(line.split('[PACKET_OVERFLOW]', 1)[1].strip())
                    except ValueError:
                        pattern_overflow += 1
                    continue
                if MARK in line or TRACE in line:
                    try:
                        is_mark = MARK in line
                        item = json.loads(line.split(MARK if is_mark else TRACE, 1)[1])
                        at = item['at'] if is_mark else item['mark_at']
                        if not start <= at < end:
                            continue
                        marker = markers.setdefault(at, {'at': at, 'label': item['label'], 'before': Counter(),
                            'after': Counter(), 'samples': {}})
                        if not is_mark:
                            target = 'before' if item['offset_ms'] < 0 else 'after'
                            marker[target][item['signature']] += 1
                            marker['samples'].setdefault(item['signature'], item['sample'][:256])
                    except (ValueError, TypeError, KeyError, IndexError):
                        pattern_overflow += 1
                    continue
                when = event_time(line)
                if when is None:
                    if collected < end:
                        logs_without_time += 1
                    continue  # Never classify an old line by its delayed collection time.
                if not start <= when < end:
                    continue
                if '[ERROR]' in line:
                    counts['errors'] += 1
                if '[ERROR]' in line and ('EW11' in line or 'socket' in line.lower() or 'receive queue' in line.lower()):
                    counts['ew11_errors'] += 1
                if 'EW11 패킷 수신 타임아웃:' in line:
                    counts['ew11_timeouts'] += 1
                if 'EW11 기기 재시작 시도' in line:
                    counts['ew11_reset_attempts'] += 1
                if 'EW11 리셋 완료' in line:
                    counts['ew11_reset_completed'] += 1
                if 'session restart count=' in line:
                    counts['session_restarts'] += 1
                if '[WATCHDOG] worker exited' in line:
                    counts['watchdog_restarts'] += 1
                changed = STATE_CHANGE.search(line)
                if changed:
                    counts[changed[1] + '_' + changed[3] + '_changes'] += 1
                match = CALL.search(line)
                if match:
                    calls.append({'at': when, 'direction': match[1], 'request_id': match[2]})
                elif ARRIVAL in line:
                    arrivals.append(when)
    ordered = sorted([(item['at'], 'call', item) for item in calls] +
                     [(at, 'arrival', None) for at in arrivals], key=lambda item: item[0])
    pending = deque()
    for at, kind, item in ordered:
        while pending and at - pending[0]['at'] > MATCH_SECONDS:
            pending.popleft()
        if kind == 'call':
            pending.append(item)
        elif len(pending) == 1:
            candidate = pending.popleft()
            candidate['arrival_at'] = at
            candidate['elapsed_seconds'] = round(at - candidate['at'], 1)
        # Zero or multiple outstanding calls cannot be linked reliably to an untagged packet.
    checkpoints = [start] + sorted(beats) + [end]
    max_gap = max(b - a for a, b in zip(checkpoints, checkpoints[1:]))
    profile_checkpoints = [start] + sorted(set(packet_windows)) + [end]
    profile_gap = max(b - a for a, b in zip(profile_checkpoints, profile_checkpoints[1:]))
    marker_reports = []
    for marker in sorted(markers.values(), key=lambda item: item['at']):
        candidates = sorted((key for key in marker['after'] if marker['after'][key] > marker['before'][key]),
                            key=lambda key: (key in seen, patterns.get(key, {}).get('known', True), -marker['after'][key]))
        marker_reports.append({'at': marker['at'], 'label': marker['label'], 'candidates': [
            {'signature': key, 'before': marker['before'][key], 'after': marker['after'][key],
             'sample': marker['samples'].get(key, '')} for key in candidates[:10]]})
    pattern_list = sorted(patterns.values(), key=lambda item: (-item['count'], item['signature']))
    return {
        'window_start': start, 'window_end': end, 'complete': bool(beats) and max_gap <= 180 and profile_gap <= 360 and not gaps and not logs_without_time and not pattern_overflow,
        'max_gap_seconds': round(max_gap), 'source_missing_samples': gaps, 'logs_without_event_time': logs_without_time,
        'packet_profile_max_gap_seconds': round(profile_gap),
        'counts': dict(counts), 'calls': calls, 'arrival_signals': len(arrivals),
        'unmatched_arrival_signals': len(arrivals) - sum('arrival_at' in c for c in calls),
        'latest_source_exists': latest_source_exists,
        'packet_patterns': pattern_list, 'new_packet_patterns': [item['signature'] for item in pattern_list if item['signature'] not in seen],
        'packet_pattern_overflow': pattern_overflow, 'packet_markers': marker_reports,
        'packet_baseline_ready': bool(seen),
    }


def format_time(timestamp):
    return dt.datetime.fromtimestamp(timestamp, KST).strftime('%m-%d %H:%M:%S')


def render(summary, max_calls=30, raw_samples=False):
    c = summary['counts']
    lines = [f"관측: {format_time(summary['window_start'])} ~ {format_time(summary['window_end'])} KST (직전 24시간)",
             '관측 상태: ' + ('완전' if summary['complete'] else '불완전 — 아래 수치는 관측된 최소 횟수'),
             f"오류 로그 전체 {c.get('errors', 0)}회 (EW11 관련 {c.get('ew11_errors', 0)}회) · EW11 패킷 타임아웃 {c.get('ew11_timeouts', 0)}회",
             f"EW11 재시작 시도 {c.get('ew11_reset_attempts', 0)}회 (완료 로그 {c.get('ew11_reset_completed', 0)}회)",
             f"통신 세션 재시작 {c.get('session_restarts', 0)}회 · 와치독 워커 재시작 {c.get('watchdog_restarts', 0)}회",
             f"엘리베이터 호출 전송 {len(summary['calls'])}회 · 도착 신호 {summary['arrival_signals']}회"]
    for call in summary['calls'][:max_calls] if max_calls is not None else summary['calls']:
        observed = (f"{format_time(call['arrival_at'])} 신호, {call['elapsed_seconds']:.1f}초 후"
                    if 'arrival_at' in call else '연결 가능한 도착 신호 없음')
        lines.append(f"- {format_time(call['at'])} {'상행' if call['direction']=='up' else '하행'}: {observed}")
    if max_calls is not None and len(summary['calls']) > max_calls:
        lines.append(f"- 나머지 {len(summary['calls'])-max_calls}회는 HA 내 저장된 보고서에서 확인")
    packet_counts = Counter()
    for item in summary.get('packet_patterns', []):
        packet_counts[DEVICE_IDS.get(item['signature'][:2], '기타')] += item['count']
    lines.append('수신 패킷(체크섬 정상): ' + ', '.join(f'{name} {count}회' for name, count in packet_counts.items())
                 if packet_counts else '수신 패킷 프로필 없음')
    lines.append(f"조명 상태 변화 {c.get('light_power_changes', 0)}회 · 온도조절 모드 변화 {c.get('thermostat_power_changes', 0)}회 · "
                 f"설정온도 변화 {c.get('thermostat_setTemp_changes', 0)}회")
    unknown = [item for item in summary.get('packet_patterns', []) if not item['known']]
    lines.append(f"해석되지 않은 패킷 유형 {len(unknown)}개 · 새 유형 {len(summary.get('new_packet_patterns', []))}개"
                 + ('' if summary.get('packet_baseline_ready') else ' (첫날 기준선 학습 중)'))
    for item in unknown[:10] if max_calls is not None else unknown:
        details = f"- 미분류 {item['signature']}: {item['count']}회"
        if item.get('hourly_counts'):
            peak_hour = max(range(24), key=lambda hour: item['hourly_counts'][hour])
            details += (f" · 최고 {peak_hour:02d}시 {item['hourly_counts'][peak_hour]}회"
                        f" · 5분 최고 {item['peak_5min_count']}회"
                        f" · 추적 변형 {len(item['tracked_variants'])}종/미추적 {item['other_variant_count']}회")
        if raw_samples and item['samples']:
            details += ' · 원문 예시 ' + ', '.join(item['samples'])
        lines.append(details)
    for marker in summary.get('packet_markers', [])[:5] if max_calls is not None else summary.get('packet_markers', []):
        lines.append(f"- {format_time(marker['at'])} {marker['label']} 실험: 후보 유형 " +
                     (', '.join(candidate['signature'] for candidate in marker['candidates'][:5]) or '없음'))
        if raw_samples:
            for candidate in marker['candidates']:
                lines.append(f"  {candidate['signature']} 전 {candidate['before']}회/후 {candidate['after']}회: {candidate['sample']}")
    if not summary['complete']:
        lines.append(f"관측 공백 최대 {summary['max_gap_seconds']}초, 원본 없음 {summary['source_missing_samples']}회, "
                     f"시각 없는 이전 로그 {summary['logs_without_event_time']}줄, 패킷 집계 누락/오류 {summary.get('packet_pattern_overflow', 0)}회, "
                     f"패킷 프로파일 공백 최대 {summary.get('packet_profile_max_gap_seconds', 0)}초")
    lines.append('도착 신호는 호출 ID가 없어 단일 대기 호출과 15분 이내에만 추정 연결합니다. 실제 탑승/도착의 증명은 아닙니다.')
    lines.append('벨·문열림 후보는 표식 전후의 상관관계일 뿐이며, 반복 실험과 대조 구간으로 확인해야 합니다.')
    return '\n'.join(lines)


def notify(token, day, message, service=''):
    if service and not NOTIFY.fullmatch(service):
        raise ValueError('only a mobile_app notify service is allowed')
    destinations = [('persistent_notification', 'create', {
        'title': 'EzVille 일일 보고 ' + day, 'message': message, 'notification_id': 'ezville_daily_' + day.replace('-', '_')})]
    if service:
        destinations.append(('notify', service.split('.', 1)[1], {'title': 'EzVille 일일 보고 ' + day, 'message': message}))
    for domain, action, payload in destinations:
        req = urllib.request.Request('http://supervisor/core/api/services/' + domain + '/' + action,
            json.dumps(payload, ensure_ascii=False).encode(),
            {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(req, timeout=15) as response:
            response.read(65536)


def initialize(db):
    db.execute('CREATE TABLE IF NOT EXISTS daily_reports (day TEXT PRIMARY KEY, status TEXT, attempted REAL, body TEXT)')
    db.execute('CREATE TABLE IF NOT EXISTS daily_state (key TEXT PRIMARY KEY, value REAL)')
    db.execute('CREATE TABLE IF NOT EXISTS packet_patterns (signature TEXT PRIMARY KEY, first_seen REAL, known INTEGER, sample TEXT)')
    db.commit()


def run_daily(database, options, now, token):
    if not options.get('daily_enabled'):
        return False
    day, start, end = window(now)
    if now < end + 90:
        return False  # Let the final 22:00 packet bucket flush and reach the collector.
    with sqlite3.connect(database) as db:
        initialize(db)
        first = db.execute("SELECT value FROM daily_state WHERE key='enabled_since'").fetchone()
        if first is None:
            db.execute("INSERT INTO daily_state VALUES ('enabled_since', ?)", (now,))
            db.commit()
            return False  # Do not send a partial past-day report when the feature is first enabled.
        if end <= first[0]:
            return False
        row = db.execute('SELECT status,attempted,body FROM daily_reports WHERE day=?', (day,)).fetchone()
        if row and (row[0] in ('done', 'running') or now - row[1] < 3600):
            return False
        # Store before delivery; crash after posting must not send a second mobile push.
        db.execute('INSERT OR REPLACE INTO daily_reports VALUES (?,?,?,?)', (day, 'running', now, '{}'))
        db.commit()
    try:
        summary = summarize(database, options['node_id'], start, end)
        message = render(summary)
        notify(token, day, message, options.get('daily_notify_service', ''))
        with sqlite3.connect(database) as db:
            db.execute('UPDATE daily_reports SET status=?,body=? WHERE day=?', ('done', json.dumps(summary, ensure_ascii=False), day))
            db.executemany('INSERT OR IGNORE INTO packet_patterns VALUES (?,?,?,?)', [
                (item['signature'], item['first'], int(item['known']), item['samples'][0] if item['samples'] else '')
                for item in summary['packet_patterns']])
        return True
    except Exception as error:
        with sqlite3.connect(database) as db:
            db.execute('UPDATE daily_reports SET status=?,body=? WHERE day=?', ('error', json.dumps({'error': type(error).__name__}), day))
        return False
