import json
import base64
import hashlib
import sys
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ezville_monitor'))
import ha_operations as ha
import remote_api
import collector


class OperationsTests(unittest.TestCase):
    def test_fixed_host_and_paths_only(self):
        with self.assertRaises(ValueError):
            ha.request('http://attacker', '/api', 'secret')
        with self.assertRaises(ValueError):
            ha.request(ha.CORE_BASE, '/../services', 'secret')
        with self.assertRaises(ValueError):
            ha.request(ha.SUPERVISOR_BASE, '/available_updates', 'secret')

    def test_supervisor_info_url_uses_real_proxy_route_and_get(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def geturl(self): return 'http://supervisor/core/info'
            def read(self, _): return b'{"result":"ok","data":{"version":"2026.10.4"}}'
        class Opener:
            def open(self, request, timeout):
                self.request = request
                self.timeout = timeout
                return Response()
        opener = Opener()
        with patch.object(ha.urllib.request, 'build_opener', return_value=opener):
            result = ha.request(ha.SUPERVISOR_BASE, '/core/info', 'secret')
        self.assertEqual(opener.request.full_url, 'http://supervisor/core/info')
        self.assertEqual(opener.request.get_method(), 'GET')
        self.assertEqual(result['version'], '2026.10.4')

    def test_logs_never_return_raw_text_or_credentials(self):
        result = ha.compact_logs({'observation': 'available', 'data': {'messages': [
            'ERROR token=VerySecretTokenABC user=Alice', 'WARNING retry']}})
        serialized = json.dumps(result)
        self.assertNotIn('VerySecretTokenABC', serialized)
        self.assertNotIn('Alice', serialized)
        self.assertEqual(result['error_codes'], [])
        self.assertEqual(result['counts']['error'], 1)

    def test_permission_and_timeout_are_not_success(self):
        with patch.object(ha, 'request', side_effect=ha.APIError('permission_denied')):
            self.assertEqual(ha.observe(ha.CORE_BASE, '/states', 't')['observation'], 'permission_denied')
        self.assertEqual(ha.compact_logs({'observation': 'timeout'}), {'observation': 'timeout'})

    def test_http_slow_drip_uses_absolute_read_deadline(self):
        class Sock:
            timeout = 0.01
            def settimeout(self, value): self.timeout = value
        class Raw: pass
        class FP: pass
        class Response:
            def __init__(self):
                self.fp = FP(); self.fp.raw = Raw(); self.fp.raw._sock = Sock()
            def read1(self, _size):
                time.sleep(min(.004, self.fp.raw._sock.timeout))
                return b'x'
        started = time.monotonic()
        with self.assertRaises(ha.APIError) as caught:
            ha._read_response(Response(), 100, .02)
        self.assertEqual(caught.exception.observation, 'timeout')
        self.assertLess(time.monotonic() - started, .05)

    def test_backup_and_disk_summary_only_exports_allowlisted_state(self):
        result = ha.summarize_states({'observation': 'available', 'data': [
            {'entity_id': 'sensor.backup_last_successful_automatic_backup', 'state': '2026-11-01T01:02:03+00:00',
             'attributes': {'password': 'NEVER_EXPORT'}},
            {'entity_id': 'sensor.backup_next_scheduled_automatic_backup', 'state': '2026-11-02T01:00:00+00:00'},
            {'entity_id': 'sensor.system_monitor_disk_free', 'state': '48.2',
             'attributes': {'unit_of_measurement': 'GB'}},
            {'entity_id': 'sensor.random_presence', 'state': 'home'}]})
        encoded = json.dumps(result)
        self.assertNotIn('NEVER_EXPORT', encoded)
        self.assertNotIn('presence', encoded)
        self.assertEqual(result['backup']['last_successful_automatic_backup']['value'], '2026-11-01T01:02:03+00:00')
        self.assertEqual(result['backup']['next_scheduled_automatic_backup']['value'], '2026-11-02T01:00:00+00:00')
        self.assertEqual(result['backup']['last_attempt_outcome']['observation'], 'unsupported')
        self.assertEqual(result['storage']['disk_free'], {'observation': 'available', 'bytes': 48200000000})
        self.assertNotIn('48.2', encoded)

    def test_backup_sensor_unavailable_does_not_imply_no_backups(self):
        result = ha.summarize_states({'observation': 'available', 'data': []})
        self.assertEqual(result['backup']['observation'], 'unsupported')
        self.assertEqual(result['backup']['fields_observed'], [])

    def test_unknown_storage_sensor_value_is_not_reported_available(self):
        result = ha.summarize_states({'observation': 'available', 'data': [
            {'entity_id': 'sensor.system_monitor_disk_free', 'state': 'VERY_PRIVATE_TEXT',
             'attributes': {'unit_of_measurement': 'GB'}}]})
        self.assertEqual(result['storage']['disk_free']['observation'], 'unavailable')
        self.assertEqual(result['storage_fields_observed'], [])
        self.assertNotIn('VERY_PRIVATE_TEXT', json.dumps(result))

    def test_disk_requires_valid_numeric_range_and_known_unit(self):
        for state, unit in [('nan', 'GB'), ('inf', 'GB'), ('-1', 'GB'), ('1e100', 'GB'), ('40.2', 'private')]:
            result = ha.summarize_states({'observation': 'available', 'data': [
                {'entity_id': 'sensor.system_monitor_disk_free', 'state': state,
                 'attributes': {'unit_of_measurement': unit}}]})
            self.assertEqual(result['storage']['disk_free']['observation'], 'unavailable', (state, unit))

    def test_backup_times_must_be_rfc3339_and_invalid_text_is_hidden(self):
        result = ha.summarize_states({'observation': 'available', 'data': [
            {'entity_id': 'sensor.backup_last_successful_automatic_backup', 'state': 'PRIVATE_NOT_A_TIMESTAMP'}]})
        self.assertEqual(result['backup']['last_successful_automatic_backup']['observation'], 'unavailable')
        self.assertNotIn('PRIVATE_NOT_A_TIMESTAMP', json.dumps(result))

    def test_addon_state_and_version_are_validated_before_export(self):
        def fake_request(base, path, token, payload=None, timeout=5):
            return {'state': 'started', 'version': 'PRIVATE_SECRET_VERSION', 'update_available': True,
                    'version_latest': '0.8.0'}
        result = ha.compact_addons({'observation': 'available'}, 't', ['core_mosquitto'], request_fn=fake_request)
        self.assertEqual(result['addons'][0]['info']['observation'], 'unavailable')
        self.assertNotIn('PRIVATE_SECRET_VERSION', json.dumps(result))

    def test_backup_history_failure_count_is_not_latest_attempt_outcome(self):
        result = ha.compact_backup({'observation': 'available', 'data': {
            'backups': [{'failed_addons': ['old failure']}], 'state': 'idle',
            'last_attempted_automatic_backup': '2026-11-01T01:00:00Z',
            'last_completed_automatic_backup': '2026-11-01T01:00:00Z'}})
        self.assertEqual(result['history_partial_failure_count'], 1)
        self.assertEqual(result['latest_attempt_outcome']['observation'], 'unsupported')
        self.assertNotIn('partial_failure_count', result)

    def test_operations_worker_timeout_keeps_packet_poll_path_independent(self):
        with ha._CACHE_LOCK:
            ha._CACHE.update(summary=None, last_attempt_at=None, last_success_at=None, last_error=None, running=False, revision=0)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=1) as executor:
            def slow(*args, **kwargs):
                time.sleep(0.25)
                return {'observation': 'available', 'core': {'version': '2026.10.4'}}
            with patch.object(ha, 'collect', side_effect=slow):
                status = ha.poll_operations('never-read-value', executor=executor, budget=0.02,
                                            interval=0, now_fn=lambda: 1000)
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 0.05)
            self.assertTrue(status['running'])
            self.assertIsNone(status['summary'])
            time.sleep(0.3)
            finished = ha.cached_status(now=1001)
            self.assertEqual(finished['revision'], 1)
            self.assertIsNotNone(finished['last_success_at'])

    def test_stuck_worker_is_reported_and_retries_are_bounded(self):
        from threading import Event
        with ha._CACHE_LOCK:
            ha._CACHE.update(summary=None, last_attempt_at=None, last_success_at=None,
                             last_error=None, running=False, revision=0)
            ha._WORKER_STARTED_MONOTONIC = None
            ha._WORKER_STUCK_BLOCKED = False
            ha._WORKER_GENERATION = 0
        entered, release = Event(), Event()
        def stuck(*_args, **_kwargs):
            entered.set(); release.wait(1)
            return {'observation': 'available', 'core': {'version': '2026.10.4'}}
        with ThreadPoolExecutor(max_workers=1) as executor, patch.object(ha, 'collect', side_effect=stuck) as collect:
            ha.poll_operations('t', executor=executor, interval=0, budget=1, now_fn=lambda: 1000)
            self.assertTrue(entered.wait(.2))
            time.sleep(.02)
            status = ha.poll_operations('t', executor=executor, interval=0, budget=1,
                                        stuck_after=.01, now_fn=lambda: 1001)
            self.assertFalse(status['running'])
            self.assertEqual(status['last_error'], 'worker_stuck')
            ha.poll_operations('t', executor=executor, interval=0, budget=1,
                               stuck_after=.01, now_fn=lambda: 1002)
            self.assertEqual(collect.call_count, 1)
            release.set()
            time.sleep(.03)
            self.assertEqual(ha.cached_status(now=1003)['last_error'], 'worker_stuck')
        with ha._CACHE_LOCK:
            ha._WORKER_STUCK_BLOCKED = False
            ha._WORKER_STARTED_MONOTONIC = None

    def test_all_sections_unavailable_does_not_refresh_last_success(self):
        with ha._CACHE_LOCK:
            ha._CACHE.update(summary={'observation': 'partial'}, last_attempt_at=None,
                             last_success_at=900, last_error=None, running=False, revision=0)
        with ThreadPoolExecutor(max_workers=1) as executor:
            with patch.object(ha, 'collect', return_value={'observation': 'unavailable'}):
                ha.poll_operations('t', executor=executor, interval=0, now_fn=lambda: 1000)
                for _ in range(50):
                    if not ha.cached_status(now=1000)['running']:
                        break
                    time.sleep(.01)
        status = ha.cached_status(now=1000)
        self.assertEqual(status['last_success_at'], 900)
        self.assertEqual(status['last_error'], 'all_unavailable')

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
        self.assertEqual(ha.compact_backup({'observation': 'available', 'data': {
            'backups': [], 'state': 'create_backup'}})['manager_state'],
            {'observation': 'available', 'value': 'create_backup'})
        self.assertEqual(ha.compact_backup({'observation': 'available', 'data': {
            'backups': [], 'state': 'private raw text'}})['manager_state'], {'observation': 'unavailable'})
        self.assertTrue(fixture['supervisor_role_default']['allows_addon_info_pattern'])
        self.assertFalse(fixture['supervisor_role_default']['allows_available_updates'])
        self.assertEqual(fixture['available_update_item_fields'], ['update_type', 'panel_path', 'version_latest'])
        self.assertEqual(fixture['implementation_paths']['websocket'], 'ws://supervisor/core/websocket')
        self.assertIn('sensor.backup_next_scheduled_automatic_backup', fixture['backup_sensors'])
        updates = ha.compact_updates({'observation': 'available', 'data': [{
            'update_type': 'addon', 'panel_path': '/hassio/store/addons/8a83e751_ezville_monitor/info',
            'version_latest': '0.8.0', 'private_panel_field': 'do-not-return'}]})
        self.assertEqual(updates['items'], [{'slug': '8a83e751_ezville_monitor', 'latest_version': '0.8.0'}])
        self.assertNotIn('panel_path', json.dumps(updates))

    def test_logs_are_bounded(self):
        result = ha.compact_logs({'observation': 'available', 'data': {'messages': ['INFO line'] * 100}})
        self.assertEqual(result['sampled_entries'], 50)
        self.assertTrue(result['truncated'])

    def test_addon_slug_is_allowlisted(self):
        def fake_request(base, path, token, payload=None, timeout=5):
            self.assertEqual(base, 'http://supervisor')
            self.assertRegex(path, r'^/addons/[a-z0-9_]+/info$')
            return {
                'state': 'started', 'version': '1', 'raw': 'private'}
        with patch.object(ha, 'request', side_effect=fake_request):
            result = ha.compact_addons({'observation': 'available'}, 't', ['core_mosquitto', '../attacker_slug'])
        self.assertEqual([a['slug'] for a in result['addons']], ['core_mosquitto'])
        self.assertNotIn('raw', json.dumps(result))

    def test_only_monitor_and_configured_controller_are_queried(self):
        paths = []
        def fake_request(base, path, token, payload=None, timeout=5):
            paths.append(path)
            return {'state': 'started', 'version': '1'}
        with patch.object(ha, 'request', side_effect=fake_request), patch.object(
                ha, 'observe_ws', return_value={'observation': 'permission_denied'}):
            ha.collect('t', request_fn=fake_request, target_slug='8a83e751_simple_mqtt_ezville_control')
        self.assertEqual(paths, [
            '/core/info', '/supervisor/info', '/os/info', '/states', '/host/info', '/resolution/info',
            '/addons/8a83e751_ezville_monitor/info',
            '/addons/8a83e751_simple_mqtt_ezville_control/info'])

    def test_sections_distinguish_not_attempted_from_unavailable(self):
        result = ha.collect('t', deadline=time.monotonic() - 1, target_slug='')
        self.assertTrue(all(not section['attempted'] for section in result['sections'].values()))
        self.assertEqual(result['sections']['core_logs']['observation'], 'timeout')
        self.assertEqual(result['sections']['addons']['observation'], 'unavailable')
        self.assertEqual(result['addons']['unattempted_count'], 1)

    def test_packet_status_is_derived_from_operations_cache(self):
        status = collector.operations_packet_status({'last_success_at': 900, 'last_error': None, 'summary': {
            'sections': {'core_info': {'attempted': True, 'observation': 'available'}}, 'core': {'version': '2026.10.4'},
            'addons': {'observation': 'partial', 'addons': [
                {'slug': 'configured_controller', 'info': {'observation': 'available',
                                                            'state': 'started', 'version': '1.2'}}]}}},
            'configured_controller', now=1000)
        self.assertEqual(status['core']['version'], '2026.10.4')
        self.assertEqual(status['addon']['version'], '1.2')
        self.assertEqual(status['addon']['observation'], 'available')

    def test_stale_or_failed_cache_does_not_become_new_available_heartbeat(self):
        status = collector.operations_packet_status({'last_success_at': 0, 'last_error': 'collector_error',
            'summary': {'sections': {'core_info': {'observation': 'available'}},
                        'core': {'version': '2026.10.4'},
                        'addons': {'addons': [{'slug': 'controller', 'info': {
                            'observation': 'available', 'state': 'started', 'version': '1.2'}}]}}},
            'controller', now=1000)
        self.assertEqual(status['observation'], 'unavailable')
        self.assertEqual(status['core']['observation'], 'unavailable')
        self.assertEqual(status['addon']['observation'], 'unavailable')
        self.assertIsNone(status['observed_at'])
        self.assertIsNone(status['core'].get('version'))

    def test_legacy_health_uses_stale_unavailable_status_not_fresh_packet_heartbeat(self):
        import sqlite3
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.sqlite'
            now = time.time()
            status = collector.operations_packet_status({'last_success_at': 0, 'last_error': 'collector_error',
                'summary': {'sections': {'core_info': {'observation': 'available'}},
                            'core': {'version': '2026.10.4'}, 'addons': {'addons': []}}}, '', now=now)
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE batches (id TEXT PRIMARY KEY,node TEXT,collected REAL,received REAL,body TEXT)')
            db.execute('INSERT INTO batches VALUES (?,?,?,?,?)', ('1','home',now,now,json.dumps({
                'events': [{'kind':'collector_heartbeat','at':now,'source_exists':True,'status':status}]})))
            db.commit(); db.close()
            health = remote_api.snapshot(str(path), 'home', now)
            self.assertEqual(health['latest_status']['observation'], 'unavailable')
            self.assertEqual(health['latest_status']['core']['observation'], 'unavailable')
            self.assertIsNone(health['latest_status']['core'].get('version'))

    def test_legacy_options_default_off_and_values_require_literal_booleans(self):
        fixture = json.loads((Path(__file__).parent / 'fixtures' / 'ha_2026_10_4_api.json').read_text())
        legacy = fixture['upgrade_0_7_1_options']
        self.assertEqual(ha.operations_options(legacy), {'collection': False, 'remote_export': False})
        config = json.loads((Path(__file__).resolve().parents[2] / 'ezville_monitor' / 'config.json').read_text())
        self.assertFalse(config['options']['operations_enabled'])
        self.assertFalse(config['options']['operations_remote_enabled'])
        self.assertEqual(ha.operations_options({'operations_enabled': True, 'remote_enabled': True}),
                         {'collection': True, 'remote_export': False})
        self.assertEqual(ha.operations_options({'operations_enabled': True,
                                                'operations_remote_enabled': True,
                                                'remote_enabled': True}),
                         {'collection': True, 'remote_export': True})
        for invalid in ('true', 1, None, {}, []):
            flags = ha.operations_options({'operations_enabled': invalid,
                                           'operations_remote_enabled': invalid,
                                           'remote_enabled': True})
            self.assertFalse(flags['collection'])
            self.assertFalse(flags['remote_export'])

    def test_local_only_options_do_not_schedule_remote_collection_path(self):
        options = {'operations_enabled': True, 'operations_remote_enabled': False, 'remote_enabled': True}
        with patch.object(ha, 'poll_operations', return_value={'revision': 1}) as poll:
            result = collector.poll_operations_if_enabled(options, 'token', None)
        poll.assert_called_once()
        self.assertEqual(result, {'revision': 1})

    def test_collection_disabled_does_not_submit_worker(self):
        with patch.object(ha, 'poll_operations') as poll:
            result = collector.poll_operations_if_enabled({'remote_enabled': True}, 'token', None)
        poll.assert_not_called()
        self.assertIsNone(result)

    def test_websocket_uses_fixed_supervisor_proxy_auth_and_read_allowlist(self):
        def server_frame(value, opcode=1, final=True):
            body = json.dumps(value, separators=(',', ':')).encode()
            return bytes(((0x80 if final else 0) | opcode, len(body))) + body
        class FakeSocket:
            def __init__(self):
                self.incoming = bytearray()
                self.sent=[]
            def settimeout(self, timeout): pass
            def sendall(self, data):
                self.sent.append(data)
                if data.startswith(b'GET '):
                    key=[line.split(b': ',1)[1] for line in data.split(b'\r\n') if line.lower().startswith(b'sec-websocket-key:')][0]
                    accept=base64.b64encode(hashlib.sha1(key+b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
                    self.incoming.extend(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n')
                    self.incoming.extend(bytes((0x89, 3)) + b'hey')
                    self.incoming.extend(server_frame({'type': 'auth_required'}))
                    self.incoming.extend(server_frame({'type': 'auth_ok'}))
                    result = json.dumps({'id': 1, 'type': 'result', 'success': True,
                                         'result': [{'message': 'private text never exported', 'level': 'ERROR'}]},
                                        separators=(',', ':')).encode()
                    split = len(result) // 2
                    self.incoming.extend(bytes((0x01, split)) + result[:split])
                    self.incoming.extend(bytes((0x80, len(result) - split)) + result[split:])
            def recv(self, size):
                data=bytes(self.incoming[:size]); del self.incoming[:size]; return data
            def close(self): pass
        sock=FakeSocket()
        result=ha.websocket_read('TOKEN_VALUE', 'system_log/list', socket_factory=lambda address, timeout: sock)
        self.assertEqual(result[0]['level'], 'ERROR')
        self.assertTrue(sock.sent[0].startswith(b'GET /core/websocket HTTP/1.1'))
        self.assertNotIn(b'/supervisor/core/websocket', sock.sent[0])
        def decode_client_frame(frame):
            opcode=frame[0]&0x0f
            length=frame[1]&0x7f; index=2
            if length==126: length=struct.unpack('!H',frame[index:index+2])[0]; index+=2
            mask=frame[index:index+4]; index+=4
            payload=bytes(byte^mask[i%4] for i,byte in enumerate(frame[index:index+length]))
            return (opcode, payload)
        outgoing=[decode_client_frame(frame) for frame in sock.sent[1:]]
        json_outgoing=[json.loads(payload) for opcode,payload in outgoing if opcode==1]
        self.assertIn({'type':'auth','api_password':'TOKEN_VALUE'},json_outgoing)
        self.assertIn({'id':1,'type':'system_log/list'},json_outgoing)
        self.assertIn((10,b'hey'),outgoing)
        with self.assertRaises(ValueError):
            ha.websocket_read('TOKEN_VALUE', 'call_service', socket_factory=lambda *_args, **_kwargs: sock)

    def test_websocket_slow_drip_obeys_absolute_deadline(self):
        class SlowSocket:
            def __init__(self): self.timeout=.01
            def settimeout(self, timeout): self.timeout=timeout
            def sendall(self, _): pass
            def recv(self, size):
                time.sleep(min(.003, self.timeout))
                return b'H'[:size]
            def close(self): pass
        with self.assertRaises(ha.APIError) as caught:
            ha.websocket_read('t', 'backup/info', timeout=.02,
                              socket_factory=lambda *_args, **_kwargs: SlowSocket())
        self.assertEqual(caught.exception.observation, 'timeout')

    def test_operations_snapshot_survives_database_reopen_and_marks_stale(self):
        import sqlite3
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.sqlite'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE batches (id TEXT PRIMARY KEY, node TEXT, collected REAL, received REAL, body TEXT)')
            db.execute('CREATE TABLE operations_snapshots (node TEXT PRIMARY KEY,collected REAL,body TEXT)')
            summary = {'observation':'partial','core': {'logs': {'observation': 'permission_denied'}}}
            status={'schema': 2, 'summary':summary,'last_attempt_at':100,'last_success_at':90,'last_error':'partial_observation','revision':2}
            db.execute('INSERT INTO batches VALUES (?,?,?,?,?)', ('1', 'home', 100, 101, json.dumps({'events':[]})))
            db.execute('INSERT INTO operations_snapshots VALUES (?,?,?)', ('home', 90, json.dumps(status)))
            db.commit()
            db.close()
            result = remote_api.operations_snapshot(str(path), 'home', {
                'remote_enabled': True, 'operations_enabled': True, 'operations_remote_enabled': True})
            self.assertEqual(result['summary'], summary)
            self.assertEqual(result['observation'], 'stale')

    def test_cache_restores_latest_summary_after_collector_restart(self):
        import sqlite3
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE operations_snapshots (node TEXT PRIMARY KEY,collected REAL,body TEXT)')
            saved = {'schema': 2, 'summary': {'schema': 2, 'core': {'version': '2026.10.4'}}, 'last_attempt_at': 100,
                     'last_success_at': 90, 'last_error': 'timeout','revision':4}
            db.execute('INSERT INTO operations_snapshots VALUES (?,?,?)', ('home', 100, json.dumps(saved)))
            with ha._CACHE_LOCK:
                ha._CACHE.update(summary=None, last_attempt_at=None, last_success_at=None, last_error=None, running=False)
            self.assertTrue(ha.restore_cache(db, 'home'))
            status = ha.cached_status(now=1000)
            self.assertEqual(status['summary'], saved['summary'])
            self.assertEqual(status['last_attempt_at'], 100)
            self.assertEqual(status['last_error'], 'timeout')

    def test_archive_migration_persists_independent_latest_snapshot(self):
        import sqlite3
        import tempfile
        import archive
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.sqlite'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE outbox (id TEXT PRIMARY KEY,body BLOB)')
            archive.initialize(db)
            status = {'observation': 'partial', 'summary': {'observation': 'partial'},
                      'last_attempt_at': 110, 'last_success_at': 100, 'revision': 3}
            body = json.dumps({'node': 'home', 'collected_at': 120, 'operations_collected_at': 110,
                               'events': [], 'operations': status}, separators=(',', ':')).encode()
            db.execute('INSERT INTO outbox VALUES (?,?)', ('batch-1', body))
            db.commit()
            archive.persist(db)
            saved = db.execute('SELECT collected,body FROM operations_snapshots WHERE node=?', ('home',)).fetchone()
            self.assertEqual(saved[0], 110)
            self.assertEqual(json.loads(saved[1]), status)
            self.assertEqual(db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
            db.close()
            reopened = sqlite3.connect(path)
            self.assertEqual(json.loads(reopened.execute('SELECT body FROM operations_snapshots WHERE node=?',
                                                          ('home',)).fetchone()[0]), status)
            reopened.close()

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
            db.execute('CREATE TABLE operations_snapshots (node TEXT PRIMARY KEY,collected REAL,body TEXT)')
            now=__import__('time').time()
            summary={'schema': 2, 'observation':'partial','core': {'version': '2026.10.4'}}
            db.execute('INSERT INTO batches VALUES (?,?,?,?,?)', ('1', 'home', now, 1, json.dumps({'events':[]})))
            db.execute('INSERT INTO operations_snapshots VALUES (?,?,?)', ('home', now, json.dumps({
                'schema': 2, 'summary':summary,'last_attempt_at':now,'last_success_at':now,'last_error':'partial_observation','revision':1})))
            db.commit()
            db.close()
            options = {'remote_enabled': True, 'operations_enabled': True,
                       'operations_remote_enabled': True}
            server = serve(str(path), 'home', 'R' * 32, options)
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

    def test_export_off_hides_cached_and_persisted_operations(self):
        import sqlite3
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.sqlite'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE batches (id TEXT PRIMARY KEY,node TEXT,collected REAL,received REAL,body TEXT)')
            db.execute('CREATE TABLE operations_snapshots (node TEXT PRIMARY KEY,collected REAL,body TEXT)')
            secret_summary = {'core': {'version': '2026.10.4', 'private': 'DO_NOT_EXPORT'}}
            db.execute('INSERT INTO operations_snapshots VALUES (?,?,?)',
                       ('home', time.time(), json.dumps({'summary': secret_summary})))
            db.commit(); db.close()
            result = remote_api.operations_snapshot(str(path), 'home', {'remote_enabled': True})
            self.assertEqual(result, {'observation': 'disabled'})
            self.assertNotIn('DO_NOT_EXPORT', json.dumps(result))

    def test_legacy_schema_one_snapshot_is_not_restored_or_exported(self):
        import sqlite3
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.sqlite'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE batches (id TEXT PRIMARY KEY,node TEXT,collected REAL,received REAL,body TEXT)')
            db.execute('CREATE TABLE operations_snapshots (node TEXT PRIMARY KEY,collected REAL,body TEXT)')
            old = {'schema': 1, 'summary': {'private': 'OLD_UNVALIDATED_SECRET'},
                   'last_attempt_at': time.time(), 'last_success_at': time.time(), 'revision': 1}
            db.execute('INSERT INTO operations_snapshots VALUES (?,?,?)', ('home', time.time(), json.dumps(old)))
            db.commit()
            with ha._CACHE_LOCK:
                ha._CACHE.update(summary={'private': 'OLD_UNVALIDATED_SECRET'}, last_success_at=time.time())
            self.assertFalse(ha.restore_cache(db, 'home'))
            self.assertIsNone(ha.cached_status()['summary'])
            db.close()
            result = remote_api.operations_snapshot(str(path), 'home', {
                'remote_enabled': True, 'operations_enabled': True, 'operations_remote_enabled': True})
            self.assertEqual(result, {'observation': 'unavailable', 'reason': 'snapshot_schema_unsupported'})
            self.assertNotIn('OLD_UNVALIDATED_SECRET', json.dumps(result))


if __name__ == '__main__':
    unittest.main()
