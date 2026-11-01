import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ezville_monitor'))
import packet_ranges
import storage


class StorageRangeTests(unittest.TestCase):
    def add_profile_window(self, conn, node, bucket, signature, count, offset=0):
        started = bucket + offset
        window = {'start': started, 'end': started + 300, 'total': count, 'signatures': 1}
        profile = {'signature': signature, 'known': False, 'count': count,
                   'first': started + 10, 'last': started + 280,
                   'bucket_start': started, 'variants': {'a' * 32: count},
                   'other_variant_count': 0, 'variant_changes': 0, 'samples': ['F799010203AA']}
        body = {'events': [
            {'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps(window)},
            {'kind': 'log', 'text': '[PACKET_PROFILE] ' + json.dumps(profile)},
            {'kind': 'collector_heartbeat', 'source_exists': True}]}
        conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', started + 301, json.dumps(body)))

    def add_incomplete_window(self, conn, bucket, with_overflow=False):
        marker = {'start': bucket, 'end': bucket + 300, 'total': 100, 'signatures': 3 if not with_overflow else 1}
        profile = {'signature': '99:01:02:00', 'known': False, 'count': 100 if not with_overflow else 5,
                   'first': bucket + 10, 'last': bucket + 280, 'bucket_start': bucket,
                   'variants': {'a' * 32: 5}, 'other_variant_count': 0, 'variant_changes': 0,
                   'samples': ['F799010203AA']}
        events = [{'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps(marker)},
                  {'kind': 'log', 'text': '[PACKET_PROFILE] ' + json.dumps(profile)}]
        if with_overflow:
            events.append({'kind': 'log', 'text': '[PACKET_OVERFLOW] 95'})
        conn.execute('INSERT INTO batches VALUES (?,?,?)',
                     ('home', bucket + 301, json.dumps({'events': events})))

    def add_complete_current_coverage(self, conn, now, excluded=()):
        end = int(now // 300) * 300
        start = end - 24 * 3600
        excluded = set(excluded)
        for bucket in range(start, end, 300):
            if bucket in excluded:
                continue
            marker = {'start': bucket, 'end': bucket + 300, 'total': 0, 'signatures': 0}
            conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', bucket + 301, json.dumps({'events': [
                {'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps(marker)}]})))
        heartbeat = json.dumps({'events': [{'kind': 'collector_heartbeat', 'source_exists': True}]})
        conn.executemany('INSERT INTO batches VALUES (?,?,?)', (
            ('home', second, heartbeat) for second in range(start, end, 60)))

    def test_discovery_uses_same_slot_and_adjacent_baseline_not_steady_polling(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 100)
                for bucket in (target - 300, target, target + 300):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', 100)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertEqual(result['candidate_total'], 0)  # steady periodic high volume is normal
            self.assertTrue(result['observation_complete'])

    def test_equal_score_candidates_prefer_newer_window_over_old_high_volume(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            end = int(now // 300) * 300
            recent = end - 600
            older = end - 4 * 3600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (older - 300, older, older + 300,
                                                               recent - 300, recent, recent + 300))
                for target, amount in ((older, 1000), (recent, 20)):
                    for day in range(1, 8):
                        self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                    for bucket, count in ((target - 300, 5), (target, amount), (target + 300, 5)):
                        self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
            result = packet_ranges.discover_candidates(db, 'home', now)
            matching = [item for item in result['candidates'] if item['bucket_start'] in (older, recent)]
            self.assertEqual([item['bucket_start'] for item in matching], [recent, older])
            self.assertEqual(matching[0]['count'], 20)

    def test_discovery_surfaces_bucket_range_for_localized_rate_spike(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count,
                                            offset=7.25 if bucket == target else 0)
                self.add_profile_window(conn, 'home', target, '99:01:02:00', 100, offset=7.25)  # duplicate ingestion is idempotent
            result = packet_ranges.discover_candidates(db, 'home', now)
            candidate = next(item for item in result['candidates'] if item['bucket_start'] == target + 7.25)
            self.assertEqual(candidate['candidate_kind'], 'local_rate_or_variant_change')
            self.assertEqual(candidate['time_precision_seconds'], 300)
            self.assertEqual(candidate['control_before'], 5)
            self.assertEqual(candidate['control_after'], 5)
            self.assertEqual(candidate['count'], 100)
            self.assertEqual(candidate['same_slot_baseline_buckets'], 7)
            self.assertNotIn('F799010203AA', json.dumps(result))
            self.assertTrue(result['observation_complete'])
            local = packet_ranges.discover_candidates(db, 'home', now, include_samples=True)
            local_candidate = next(item for item in local['candidates'] if item['bucket_start'] == target + 7.25)
            self.assertEqual(local_candidate['raw_samples'], ['F799010203AA'])
            self.assertEqual(candidate['bucket_start'], target + 7.25)
            self.assertEqual(candidate['bucket_end'], target + 307.25)
            self.assertEqual(candidate['same_slot_baseline_days_observed'], 7)
            self.assertEqual(candidate['baseline_days_configured'], 7)

    def test_discovery_does_not_treat_marker_first_partial_profile_flush_as_zero_baseline(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target,))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                self.add_profile_window(conn, 'home', target - 300, '99:01:02:00', 5)
                self.add_incomplete_window(conn, target)  # marker promises 3 profiles, only first has arrived
                self.add_profile_window(conn, 'home', target + 300, '99:01:02:00', 5)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(result['observation_complete'])
            self.assertGreaterEqual(result['incomplete_profile_windows'], 1)
            self.assertGreaterEqual(result['missing_profile_buckets'], 1)
            self.assertFalse(any(item['bucket_start'] == target for item in result['candidates']))

    def test_discovery_suppresses_overflowed_flush_instead_of_treating_missing_profiles_as_zero(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target,))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                self.add_profile_window(conn, 'home', target - 300, '99:01:02:00', 5)
                self.add_incomplete_window(conn, target, with_overflow=True)
                self.add_profile_window(conn, 'home', target + 300, '99:01:02:00', 5)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(result['observation_complete'])
            self.assertEqual(result['overflowed_profile_windows'], 1)
            self.assertFalse(any(item['bucket_start'] == target for item in result['candidates']))

    def test_unrelated_incomplete_historical_bucket_does_not_suppress_complete_local_candidate(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    prior = target - day * 86400
                    if day == 7:
                        self.add_incomplete_window(conn, prior)
                    else:
                        self.add_profile_window(conn, 'home', prior, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
            result = packet_ranges.discover_candidates(db, 'home', now)
            candidate = next((item for item in result['candidates'] if item['bucket_start'] == target), None)
            self.assertIsNotNone(candidate, 'unrelated historical incompleteness should not block local evidence')
            self.assertEqual(candidate['same_slot_baseline_buckets'], 6)
            self.assertTrue(result['current_window_complete'])
            self.assertGreater(result['incomplete_profile_windows'], 0)

    def test_unrelated_current_gap_does_not_suppress_candidate_with_complete_local_evidence(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            unrelated = target - 6 * 3600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300, unrelated))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
            result = packet_ranges.discover_candidates(db, 'home', now)
            candidate = next((item for item in result['candidates'] if item['bucket_start'] == target), None)
            self.assertIsNotNone(candidate)
            self.assertFalse(result['current_window_complete'])
            self.assertFalse(result['observation_complete'])
            self.assertTrue(candidate['candidate_evidence_complete'])
            self.assertEqual(result['missing_profile_buckets'], 1)

    def test_sustained_new_signature_is_consolidated_as_one_episode(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            signature = '99:01:02:00'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for bucket in (target - 300, target, target + 300):
                    for day in range(1, 8):
                        self.add_profile_window(conn, 'home', bucket - day * 86400, signature, 0)
                for bucket in (target - 300, target, target + 300):
                    self.add_profile_window(conn, 'home', bucket, signature, 100)
            result = packet_ranges.discover_candidates(db, 'home', now)
            sustained = [item for item in result['candidates'] if item['signature'] == signature]
            self.assertEqual(len(sustained), 1)
            self.assertEqual(sustained[0]['candidate_kind'], 'sustained_new_signature_episode')
            self.assertEqual(sustained[0]['episode_bucket_count'], 3)
            self.assertEqual(sustained[0]['episode_bucket_start'], target - 300)
            self.assertEqual(sustained[0]['episode_bucket_end'], target + 600)

    def test_isolated_new_signature_remains_a_single_bucket_candidate(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            signature = '99:01:02:00'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target,))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, signature, 0)
                self.add_profile_window(conn, 'home', target, signature, 10)
            result = packet_ranges.discover_candidates(db, 'home', now)
            selected = [item for item in result['candidates'] if item['signature'] == signature]
            self.assertEqual(len(selected), 1)
            self.assertEqual(selected[0]['candidate_kind'], 'new_signature')
            self.assertNotIn('episode_bucket_count', selected[0])

    def test_zero_profiles_and_packet_overflow_make_reported_window_incomplete(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            end = int(now // 300) * 300
            start = end - 24 * 3600
            target = start + 12 * 3600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                for second in range(start, end, 60):
                    conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', second,
                        json.dumps({'events': [{'kind': 'collector_heartbeat', 'source_exists': True}]})))
                for bucket in range(start, end, 300):
                    is_overflow = bucket == target
                    events = [{'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps({
                        'start': bucket, 'end': bucket + 300,
                        'total': 100 if is_overflow else 0, 'signatures': 3 if is_overflow else 0})}]
                    if is_overflow:
                        events.append({'kind': 'log', 'text': '[PACKET_OVERFLOW] 100'})
                    conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', bucket + 301,
                        json.dumps({'events': events})))
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(result['observation_complete'])
            self.assertEqual(result['candidate_total'], 0)
            self.assertEqual(result['missing_profile_buckets'], 1)
            self.assertEqual(result['incomplete_profile_windows'], 1)
            self.assertEqual(result['overflowed_profile_windows'], 1)
            self.assertEqual(result['baseline_days_configured'], 7)
            self.assertFalse(any('same_slot_baseline_days_observed' in item for item in result['candidates']))

    def test_physical_storage_counts_rotations_and_wal_separately_from_filesystem(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            db = root / 'data.sqlite'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
            (root / 'log').write_bytes(b'x' * 100)
            (root / 'log.1').write_bytes(b'x' * 50)
            (root / 'unrelated').write_bytes(b'x' * 1000)
            result = storage.measure(db, {'log_path': str(root / 'log')}, 100)
            self.assertEqual(result['source_logs']['file_bytes'], 150)
            self.assertEqual(result['source_logs']['file_count'], 2)
            self.assertGreater(result['monitor_archive']['logical_bytes'], 0)
            self.assertIn('available_bytes', result['monitor_filesystem'])
            self.assertNotIn(str(root), json.dumps(result))
            (root / 'log.1').unlink()
            after = storage.measure(db, {'log_path': str(root / 'log')}, 200, result)
            self.assertEqual(after['source_logs']['retained_change_bytes'], -50)
            with patch.object(storage.os, 'statvfs', side_effect=OSError):
                self.assertFalse(storage.disk(root)['available'])

    def test_range_marks_overflow_and_missing_coverage_without_exporting_raw_frames(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                profiles = []
                for i in range(1100):
                    item = {'signature': '99:01:02:03', 'known': False, 'count': 1,
                            'first': 100, 'last': 101, 'samples': ['PRIVATE_RAW']}
                    profiles.append({'kind': 'log', 'text': '[PACKET_PROFILE] ' + json.dumps(item)})
                conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', 150, json.dumps({'events': profiles})))
            result = packet_ranges.read(db, 'home', 90, 160, 200)
            self.assertTrue(result['truncated'])
            self.assertFalse(result['observation_complete'])
            self.assertLessEqual(len(json.dumps(result).encode()), 200000)
            self.assertNotIn('PRIVATE_RAW', json.dumps(result))

    def test_candidate_scan_marks_actual_limit_overflow_and_keeps_recent_rows(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            end = int(now // 300) * 300
            start = end - 24 * 3600
            heartbeat = json.dumps({'events': [{'kind': 'collector_heartbeat', 'source_exists': True}]})
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                conn.executemany('INSERT INTO batches VALUES (?,?,?)', (
                    ('home', start + index * 2, heartbeat)
                    for index in range(packet_ranges.MAX_SCAN_BATCHES)))
            exact = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(exact['truncated'])  # exactly the declared cap is complete with respect to scan size
            self.assertEqual(exact['batches_scanned'], packet_ranges.MAX_SCAN_BATCHES)
            self.assertEqual(exact['older_rows_omitted_at_least'], 0)
            with sqlite3.connect(db) as conn:
                history_start = start - 7 * 86400
                conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', history_start + 1, heartbeat))
            overflow = packet_ranges.discover_candidates(db, 'home', now)
            self.assertTrue(overflow['truncated'])
            self.assertEqual(overflow['matching_rows_detected_at_least'], packet_ranges.MAX_SCAN_BATCHES + 1)
            self.assertEqual(overflow['older_rows_omitted_at_least'], 1)
            self.assertFalse(overflow['observation_complete'])
            self.assertLess(overflow['collector_max_gap_seconds'], 86400)  # latest current-window rows were retained
