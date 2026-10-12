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
import receiver
import weekly
import health


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

    def test_duplicate_ack_and_report_gaps(self):
        self.log.write_text('[ERROR] failure\n')
        batch = self.collect()
        database = self.root / 'received.sqlite'
        with receiver.connect(database) as db:
            for _ in range(2):
                self.assertEqual(receiver.accept(db, protocol.encode(batch), 1000), batch['id'])
            self.assertEqual(db.execute('SELECT count(*) FROM batches').fetchone()[0], 1)
        result = weekly.report(database, 'home', 1100)
        self.assertEqual(result['counts']['error'], 1)
        self.assertFalse(result['observation_complete'])

    def test_complete_short_window(self):
        self.log.write_text('ok\n')
        database = self.root / 'received.sqlite'
        with receiver.connect(database) as db:
            for now in range(1000, 1601, 60):
                receiver.accept(db, protocol.encode(self.collect(now)), now)
        result = weekly.report(database, 'home', 1600, days=600/86400)
        self.assertTrue(result['observation_complete'])

    def test_patch_rejects_commands_and_workflow_changes(self):
        for filename in ['.github/workflows/publish.yml', 'simple_mqtt_ezville_control/config.json', '../escape']:
            with self.assertRaises(ValueError):
                weekly.validate_patch(self.root, 'diff --git a/{0} b/{0}\n'.format(filename))

    def test_failed_delivery_keeps_outbox(self):
        self.log.write_text('one\n')
        self.collect()
        with self.assertRaises(ValueError):
            collector.send_pending(self.db, 'http://insecure/ingest', 'secret')
        self.assertEqual(self.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)

    def test_http_authenticated_ingest(self):
        secret = 's' * 32
        server = receiver.ThreadingHTTPServer(('127.0.0.1', 0), receiver.handler(self.root / 'received.sqlite', secret))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            self.log.write_text('one\n')
            body = protocol.encode(self.collect(time.time()))
            timestamp = str(int(time.time()))
            url = 'http://127.0.0.1:{}/ingest'.format(server.server_port)
            request = urllib.request.Request(url, body, headers={'X-Ezville-Time': timestamp,
                                             'X-Ezville-Signature': protocol.signature(secret, timestamp, body)})
            with urllib.request.urlopen(request) as result:
                self.assertIn('accepted', json.load(result))
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(urllib.request.Request(url, body))
            self.assertEqual(error.exception.code, 401)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_stale_stream_is_detected_externally(self):
        self.log.write_text('one\n')
        database = self.root / 'received.sqlite'
        with receiver.connect(database) as db:
            receiver.accept(db, protocol.encode(self.collect()), 1000)
        self.assertEqual(health.check(database, 'home', 1200)['status'], 'stale')
        self.assertEqual(health.check(database, 'other', 1200)['status'], 'unknown')

    def test_batch_id_collision_is_not_acknowledged(self):
        batch = self.collect()
        with receiver.connect(self.root / 'received.sqlite') as db:
            receiver.accept(db, protocol.encode(batch), 1000)
            batch['node'] = 'changed'
            with self.assertRaises(ValueError):
                receiver.accept(db, protocol.encode(batch), 1000)

    def test_future_collection_rejected(self):
        with receiver.connect(self.root / 'received.sqlite') as db:
            with self.assertRaises(ValueError):
                receiver.accept(db, protocol.encode(self.collect(2000)), 1000)

    def test_supervisor_unavailable_not_healthy(self):
        with patch('urllib.request.urlopen', side_effect=OSError):
            result = collector.supervisor_status('installed_slug', 'private-token')
        self.assertEqual(result['core']['observation'], 'unavailable')
        self.assertNotIn('private-token', json.dumps(result))

    def test_patch_application_allowed_source_only(self):
        import subprocess
        repo = self.root / 'repo'
        source = repo / 'simple_mqtt_ezville_control/ezville.py'
        source.parent.mkdir(parents=True)
        source.write_text('value = 1\n')
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        subprocess.run(['git', 'add', '.'], cwd=repo, check=True)
        subprocess.run(['git', '-c', 'user.name=test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'base'], cwd=repo, check=True)
        filename = 'simple_mqtt_ezville_control/ezville.py'
        diff = 'diff --git a/{0} b/{0}\n--- a/{0}\n+++ b/{0}\n@@ -1 +1 @@\n-value = 1\n+value = 2\n'.format(filename)
        weekly.validate_patch(repo, diff)
        self.assertEqual(source.read_text(), 'value = 2\n')


if __name__ == '__main__':
    unittest.main()
