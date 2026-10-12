import importlib.util
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'ezville_monitor'))
sys.path.insert(0, str(ROOT / 'monitoring'))
import collector
import protocol
import archive
import maintenance


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.log = self.root / 'ezville.log'
        self.db = collector.connect(self.root / 'outbox.sqlite')

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def collect(self, now=1000):
        return collector.collect(self.db, str(self.log), 'home', now)

    def logs(self, batch):
        return [e['text'] for e in batch['events'] if e['kind'] == 'log']

    def test_cursor_no_duplicate_and_partial_line(self):
        self.log.write_text('one\npartial')
        self.assertEqual(self.logs(self.collect()), ['one\n'])
        self.assertEqual(self.logs(self.collect()), [])
        with self.log.open('a') as f:
            f.write(' line\n')
        self.assertEqual(self.logs(self.collect()), ['partial line\n'])

    def test_rotation_tracks_inode(self):
        self.log.write_text('old\n')
        self.collect()
        self.log.rename(str(self.log) + '.2026-10-12')
        self.log.write_text('new\n')
        self.assertEqual(self.logs(self.collect()), ['new\n'])

    def test_truncation_records_gap(self):
        self.log.write_text('long old line\n')
        self.collect()
        self.log.write_text('x\n')
        batch = self.collect()
        self.assertTrue(any(e['kind'] == 'gap' for e in batch['events']))
        self.assertEqual(self.logs(batch), ['x\n'])

    def test_persistent_outbox_and_cursors(self):
        self.log.write_text('old\n')
        self.collect()
        self.db.close()
        self.db = collector.connect(self.root / 'outbox.sqlite')
        self.assertEqual(self.logs(self.collect()), [])
        self.assertEqual(self.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 2)

    def test_backpressure_does_not_advance_cursor(self):
        self.log.write_text('old\n')
        self.collect()
        with self.log.open('a') as f:
            f.write('new\n')
        with self.assertRaises(RuntimeError):
            collector.collect(self.db, str(self.log), 'home', 1001, max_batches=1)
        self.assertEqual(self.logs(self.collect()), ['new\n'])

    def test_no_stack_dump_export(self):
        self.log.write_text('ok\n')
        Path(str(self.log) + '.stacks.log').write_text('sensitive stack\n')
        self.assertEqual(self.logs(self.collect()), ['ok\n'])

    def test_missing_source_is_unknown(self):
        event = self.collect()['events'][-1]
        self.assertFalse(event['source_exists'])
        self.assertEqual(event['status']['observation'], 'unknown')

    def test_redaction(self):
        for text in ['password=secretvalue', 'Bearer secretvalue', 'token:secretvalue',
                     'https://user:secretvalue@host/path']:
            self.assertNotIn('secretvalue', protocol.redact(text))

    def test_hmac_tampering_expiration(self):
        body = b'example'
        signed = protocol.signature('secret', '1000', body)
        self.assertTrue(protocol.authenticated('secret', '1000', body, signed, 1000))
        self.assertFalse(protocol.authenticated('secret', '1000', body+b'x', signed, 1000))
        self.assertFalse(protocol.authenticated('secret', '1000', body, signed, 1301))
        self.assertFalse(protocol.authenticated('secret', None, body, signed, 1000))

    def test_supervisor_unavailable_not_healthy(self):
        with patch('urllib.request.urlopen', side_effect=OSError):
            result = collector.supervisor_status('installed_slug', 'private-token')
        self.assertEqual(result['core']['observation'], 'unavailable')
        self.assertNotIn('private-token', json.dumps(result))



if __name__ == '__main__':
    unittest.main()
