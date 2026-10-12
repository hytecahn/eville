import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ezville_monitor'))
import daily


def kst(day, hour=22):
    return dt.datetime.fromisoformat(day).replace(hour=hour, tzinfo=daily.KST).timestamp()


class DailyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / 'daily.sqlite'
        with sqlite3.connect(self.db) as conn:
            conn.execute('CREATE TABLE batches (id TEXT PRIMARY KEY, node TEXT, collected REAL, received REAL, body TEXT)')
        self.end = kst('2026-10-13')
        self.start = self.end - 86400
        self.counter = 0

    def tearDown(self):
        self.temp.cleanup()

    def append(self, when, lines=(), heartbeat=True):
        events = [{'kind': 'log', 'source': 'simple_mqtt_ezville_control.log',
                   'text': f'[ts_ms={int(at * 1000)}] {line}'} for at, line in lines]
        if heartbeat:
            events.append({'kind': 'collector_heartbeat', 'source_exists': True})
        self.counter += 1
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO batches VALUES (?,?,?,?,?)',
                         (str(self.counter), 'home', when, when, json.dumps({'events': events})))

    def test_window_is_last_24_hours_at_22_kst(self):
        day, start, end = daily.window(self.end + 5)
        self.assertEqual(day, '2026-10-13')
        self.assertEqual((start, end), (self.start, self.end))

    def test_counts_call_and_arrival_as_estimate_without_duplicating_retries(self):
        self.append(self.start + 100, [(self.start + 100, '[ERROR] EW11 기기 재시작 오류: test'),
                                       (self.start + 100, '[WARNING] EW11 패킷 수신 타임아웃: 3600초'),
                                       (self.start + 100, '[INFO] EW11 기기 재시작 시도'),
                                       (self.start + 100, '[WARNING] session restart count=1, retry in 1s'),
                                       (self.start + 100, '[WATCHDOG] event loop stalled for 120s; restarting worker'),
                                       (self.start + 100, '[WATCHDOG] worker exited 1; restart in 1s')])
        self.append(self.start + 200, [(self.start + 200, '[EVENT] elevator_call_sent direction=up request_id=' + 'a' * 32)])
        self.append(self.start + 243.25, [(self.start + 243.25, '[EVENT] elevator_arrival_signal')])
        report = daily.summarize(self.db, 'home', self.start, self.end)
        self.assertEqual(report['counts']['ew11_errors'], 1)
        self.assertEqual(report['counts']['errors'], 1)
        self.assertEqual(report['counts']['ew11_timeouts'], 1)
        self.assertEqual(report['counts']['ew11_reset_attempts'], 1)
        self.assertEqual(report['counts']['session_restarts'], 1)
        self.assertEqual(report['counts']['watchdog_restarts'], 1)
        self.assertEqual(report['calls'][0]['elapsed_seconds'], 43.2)
        self.assertFalse(report['complete'])
        self.assertIn('실제 탑승/도착의 증명은 아닙니다', daily.render(report))

    def test_overlapping_calls_and_legacy_lines_are_not_falsely_attributed(self):
        self.append(self.start + 100, [(self.start + 100, '[EVENT] elevator_call_sent direction=up request_id=' + 'a' * 32),
                                       (self.start + 101, '[EVENT] elevator_call_sent direction=down request_id=' + 'b' * 32),
                                       (self.start + 140, '[EVENT] elevator_arrival_signal')])
        self.counter += 1
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO batches VALUES (?,?,?,?,?)',
                         (str(self.counter), 'home', self.start + 150, self.start + 150,
                          json.dumps({'events': [{'kind': 'log', 'text': '[2026-10-13 PM 02:00:00] [ERROR] EW11 old'}]})))
        report = daily.summarize(self.db, 'home', self.start, self.end)
        self.assertEqual(report['unmatched_arrival_signals'], 1)
        self.assertTrue(all('elapsed_seconds' not in c for c in report['calls']))
        self.assertEqual(report['logs_without_event_time'], 1)

    def test_sends_one_report_and_never_resends_on_next_poll(self):
        self.append(self.start + 60)
        sent = []
        with patch.object(daily, 'notify', side_effect=lambda *args: sent.append(args)):
            options = {'daily_enabled': True, 'node_id': 'home'}
            self.assertFalse(daily.run_daily(self.db, options, self.end - 3600, 'test'))
            self.assertFalse(daily.run_daily(self.db, options, self.end + 10, 'test'))
            self.assertTrue(daily.run_daily(self.db, options, self.end + 100, 'test'))
            self.assertFalse(daily.run_daily(self.db, options, self.end + 170, 'test'))
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][1], '2026-10-13')

    def test_notification_target_rejects_arbitrary_service(self):
        with self.assertRaises(ValueError):
            daily.notify('token', '2026-10-13', 'report', 'homeassistant.restart')

    def test_unknown_packet_and_marked_experiment_are_local_evidence(self):
        at = self.end - 60
        profile = {'signature': '99:01:02:00', 'known': False, 'count': 81,
                   'first': at, 'last': at + 20, 'samples': ['F799010200AABB']}
        mark = {'label': 'bell', 'at': at + 2}
        before = {'label': 'bell', 'mark_at': at + 2, 'offset_ms': -1000,
                  'signature': '0E:11:81:04', 'sample': 'F70E118104'}
        after = {'label': 'bell', 'mark_at': at + 2, 'offset_ms': 1000,
                 'signature': '99:01:02:00', 'sample': 'F799010200AABB'}
        self.append(self.end + 3, [(self.end + 3, '[PACKET_PROFILE] ' + json.dumps(profile)),
                                    (self.end + 3, '[PACKET_MARK] ' + json.dumps(mark)),
                                    (self.end + 3, '[PACKET_TRACE] ' + json.dumps(before)),
                                    (self.end + 3, '[PACKET_TRACE] ' + json.dumps(after))], heartbeat=False)
        report = daily.summarize(self.db, 'home', self.start, self.end)
        self.assertEqual(report['packet_patterns'][0]['count'], 81)
        self.assertEqual(report['new_packet_patterns'], ['99:01:02:00'])
        self.assertEqual(report['packet_markers'][0]['candidates'][0]['signature'], '99:01:02:00')
        self.assertNotIn('F799', daily.render(report))
        self.assertIn('F799', daily.render(report, raw_samples=True))

    def test_profile_hourly_rates_and_bounded_variant_breakdown(self):
        fingerprint = hashlib.sha256(b'F799010200AA').hexdigest()[:32]
        for offset, count in [(120, 7), (3720, 20)]:
            at = self.start + offset
            profile = {'signature': '99:01:02:00', 'known': False, 'count': count,
                       'first': at, 'last': at + 20, 'bucket_start': at - 20,
                       'samples': ['F799010200AA'], 'variants': {fingerprint: count - 2},
                       'other_variant_count': 2, 'variant_changes': 3}
            self.append(at + 30, [(at + 30, '[PACKET_PROFILE] ' + json.dumps(profile))])
        item = daily.summarize(self.db, 'home', self.start, self.end)['packet_patterns'][0]
        self.assertEqual(item['count'], 27)
        self.assertEqual(item['hourly_counts'][22:24], [7, 20])
        self.assertEqual(item['active_buckets'], 2)
        self.assertEqual(item['peak_5min_count'], 20)
        self.assertEqual(item['tracked_variants'][fingerprint], 23)
        self.assertEqual(item['other_variant_count'], 4)
        self.assertEqual(item['variant_changes'], 6)


if __name__ == '__main__':
    unittest.main()
