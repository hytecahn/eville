import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ezville_monitor'))
import ha_operations as ha
import remote_api


class OperationsTests(unittest.TestCase):
    def test_fixed_host_and_paths_only(self):
        with self.assertRaises(ValueError):
            ha.request('http://attacker', '/api', 'secret')
        with self.assertRaises(ValueError):
            ha.request(ha.CORE_BASE, '/../services', 'secret')

    def test_logs_never_return_raw_text_or_credentials(self):
        result = ha.compact_logs({'observation': 'available', 'data': {'messages': [
            'ERROR failed token=abc123 device at 192.168.1.5', 'WARNING retry']}})
        serialized = json.dumps(result)
        self.assertNotIn('abc123', serialized)
        self.assertNotIn('192.168.1.5', serialized)
        self.assertNotIn('failed', serialized)
        self.assertEqual(result['counts']['error'], 1)

    def test_permission_and_timeout_are_not_success(self):
        with patch.object(ha, 'request', side_effect=ha.APIError('permission_denied')):
            self.assertEqual(ha.observe(ha.CORE_BASE, '/states', 't')['observation'], 'permission_denied')
        self.assertEqual(ha.compact_logs({'observation': 'timeout'}), {'observation': 'timeout'})

    def test_backup_and_disk_summary_only_exports_allowlisted_state(self):
        result = ha.summarize_states({'observation': 'available', 'data': [
            {'entity_id': 'sensor.backup_last_successful_automatic_backup', 'state': '2026-11-01T01:02:03+00:00',
             'attributes': {'password': 'NEVER_EXPORT'}},
            {'entity_id': 'sensor.backup_next_scheduled_automatic_backup', 'state': '2026-11-02T01:00:00+00:00'},
            {'entity_id': 'sensor.system_monitor_disk_free', 'state': '48.2'},
            {'entity_id': 'sensor.random_presence', 'state': 'home'}]})
        encoded = json.dumps(result)
        self.assertNotIn('NEVER_EXPORT', encoded)
        self.assertNotIn('presence', encoded)
        self.assertEqual(result['backup']['last_successful_automatic_backup'], '2026-11-01T01:02:03+00:00')
        self.assertEqual(result['backup']['next_scheduled_automatic_backup'], '2026-11-02T01:00:00+00:00')
        self.assertEqual(result['backup']['last_attempt_outcome']['observation'], 'unsupported')

    def test_backup_sensor_unavailable_does_not_imply_no_backups(self):
        result = ha.summarize_states({'observation': 'available', 'data': []})
        self.assertEqual(result['backup']['observation'], 'unsupported')
        self.assertEqual(result['backup']['fields_observed'], [])

    def test_operations_worker_timeout_keeps_packet_poll_path_independent(self):
        with ha._CACHE_LOCK:
            ha._CACHE.update(summary=None, last_attempt_at=None, last_success_at=None, last_error=None, running=False)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=1) as executor:
            original = ha.collect
            def slow(*args, **kwargs):
                time.sleep(0.15)
                return {'slow': True}
            with patch.object(ha, 'collect', side_effect=slow):
                status = ha.poll_operations('never-read-value', executor=executor, budget=0.02,
                                            interval=0, now_fn=lambda: 1000)
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 0.2)
            self.assertEqual(status['last_error'], 'timeout')
            self.assertIsNone(status['summary'])

    def test_worker_deduplicates_concurrent_polls_and_reports_cached_age(self):
        with ha._CACHE_LOCK:
            ha._CACHE.update(summary={'ok': True}, last_attempt_at=100, last_success_at=90,
                              last_error=None, running=True)
        status = ha.cached_status(now=5000)
        self.assertEqual(status['observation'], 'stale')
        self.assertEqual(status['data_age_seconds'], 4910)
        self.assertTrue(status['running'])

    def test_api_version_fixture_covers_ha_2026_10_4_contract(self):
        fixture = json.loads((Path(__file__).parent / 'fixtures' / 'ha_2026_10_4_api.json').read_text())
        self.assertEqual(fixture['home_assistant_core'], '2026.10.4')
        self.assertIn('persistent_notification/get', fixture['websocket_admin_or_auth_required'])
        self.assertIn('backup/info', fixture['websocket_admin_or_auth_required'])
        self.assertTrue(fixture['backup/info']['admin_required'])
        self.assertIn('sensor.backup_next_scheduled_automatic_backup', fixture['backup_sensors'])

    def test_logs_are_bounded(self):
        result = ha.compact_logs({'observation': 'available', 'data': {'messages': ['INFO line'] * 100}})
        self.assertEqual(result['sampled_entries'], 50)
        self.assertTrue(result['truncated'])

    def test_addon_slug_is_allowlisted(self):
        def fake_request(base, path, token, payload=None, timeout=5):
            return {'addons': [{'slug': 'attacker_slug'}, {'slug': 'core_mosquitto'}]} if path == '/addons' else {
                'state': 'started', 'version': '1', 'raw': 'private'}
        with patch.object(ha, 'request', side_effect=fake_request):
            result = ha.compact_addons({'observation': 'available', 'data': fake_request(ha.SUPERVISOR_BASE, '/addons', 't')}, 't')
        self.assertEqual([a['slug'] for a in result['addons']], ['core_mosquitto'])
        self.assertNotIn('raw', json.dumps(result))

    def test_operations_snapshot_survives_database_reopen_and_marks_stale(self):
        import sqlite3
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.sqlite'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE batches (id TEXT PRIMARY KEY, node TEXT, collected REAL, received REAL, body TEXT)')
            summary = {'core': {'logs': {'observation': 'permission_denied'}}}
            db.execute('INSERT INTO batches VALUES (?,?,?,?,?)', ('1', 'home', 100, 101,
                       json.dumps({'operations': summary})))
            db.commit()
            db.close()
            result = remote_api.operations_snapshot(str(path), 'home')
            self.assertEqual(result['summary'], summary)
            self.assertEqual(result['observation'], 'stale')

    def test_cache_restores_latest_summary_after_collector_restart(self):
        import sqlite3
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE batches (node TEXT, collected REAL, body TEXT)')
            saved = {'summary': {'core': {'version': '2026.10.4'}}, 'last_attempt_at': 100,
                     'last_success_at': 90, 'last_error': 'timeout'}
            db.execute('INSERT INTO batches VALUES (?,?,?)', ('home', 100, json.dumps({'operations': saved})))
            with ha._CACHE_LOCK:
                ha._CACHE.update(summary=None, last_attempt_at=None, last_success_at=None, last_error=None, running=False)
            self.assertTrue(ha.restore_cache(db, 'home'))
            status = ha.cached_status(now=1000)
            self.assertEqual(status['summary'], saved['summary'])
            self.assertEqual(status['last_attempt_at'], 100)
            self.assertEqual(status['last_error'], 'timeout')

    def test_remote_operations_route_is_authenticated_and_fixed_read(self):
        import sqlite3
        import tempfile
        import threading
        import urllib.request
        from remote_api import serve, sign
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.sqlite'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE batches (id TEXT PRIMARY KEY, node TEXT, collected REAL, received REAL, body TEXT)')
            db.execute('INSERT INTO batches VALUES (?,?,?,?,?)', ('1', 'home', __import__('time').time(), 1,
                       json.dumps({'operations': {'core': {'version': '2026.10.4'}}})))
            db.commit()
            db.close()
            server = serve(str(path), 'home', 'R' * 32)
            try:
                port = server.server_address[1]
                payload = b'{}'
                stamp, nonce = str(int(__import__('time').time())), 'a' * 32
                req = urllib.request.Request(f'http://127.0.0.1:{port}/v1/operations', payload, method='POST', headers={
                    'X-Ezville-Time': stamp, 'X-Ezville-Nonce': nonce,
                    'X-Ezville-Signature': sign('R' * 32, 'POST', '/v1/operations', stamp, nonce, payload)})
                with urllib.request.urlopen(req, timeout=3) as response:
                    self.assertEqual(json.loads(response.read())['summary']['core']['version'], '2026.10.4')
            finally:
                server.shutdown()
                server.server_close()


if __name__ == '__main__':
    unittest.main()
