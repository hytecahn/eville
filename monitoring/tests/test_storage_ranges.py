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
    def add_profile_window(self, conn, node, bucket, signature, count):
        window = {'start': bucket, 'end': bucket + 300, 'total': count, 'signatures': 1}
        profile = {'signature': signature, 'known': False, 'count': count,
                   'first': bucket + 10, 'last': bucket + 280,
                   'bucket_start': bucket, 'variants': {'a' * 32: count},
                   'other_variant_count': 0, 'variant_changes': 0, 'samples': ['F799010203AA']}
        body = {'events': [
            {'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps(window)},
            {'kind': 'log', 'text': '[PACKET_PROFILE] ' + json.dumps(profile)},
            {'kind': 'collector_heartbeat', 'source_exists': True}]}
        conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', bucket + 301, json.dumps(body)))

    def test_discovery_uses_same_slot_and_adjacent_baseline_not_steady_polling(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 100)
                for bucket in (target - 300, target, target + 300):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', 100)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertEqual(result['candidate_total'], 0)  # steady periodic high volume is normal

    def test_discovery_surfaces_bucket_range_for_localized_rate_spike(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
                self.add_profile_window(conn, 'home', target, '99:01:02:00', 100)  # duplicate ingestion is idempotent
            result = packet_ranges.discover_candidates(db, 'home', now)
            candidate = next(item for item in result['candidates'] if item['bucket_start'] == target)
            self.assertEqual(candidate['candidate_kind'], 'local_rate_or_variant_change')
            self.assertEqual(candidate['time_precision_seconds'], 300)
            self.assertEqual(candidate['control_before'], 5)
            self.assertEqual(candidate['control_after'], 5)
            self.assertEqual(candidate['count'], 100)
            self.assertEqual(candidate['same_slot_baseline_buckets'], 7)
            self.assertNotIn('F799010203AA', json.dumps(result))
            self.assertFalse(result['observation_complete'])  # sparse fixtures must say coverage is incomplete
            local = packet_ranges.discover_candidates(db, 'home', now, include_samples=True)
            local_candidate = next(item for item in local['candidates'] if item['bucket_start'] == target)
            self.assertEqual(local_candidate['raw_samples'], ['F799010203AA'])

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
