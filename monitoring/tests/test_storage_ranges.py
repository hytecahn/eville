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
