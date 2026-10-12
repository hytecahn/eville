import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import re
import secrets
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ezville_monitor'))
import archive
import collector
import remote_api
import daily
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import read_remote
import run_daily_remote


class RemoteTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.database=self.root/'data.sqlite'
        self.log=self.root/'source.log'
        self.log.write_text('[HEALTH] real source one\n')
        self.secret='s'*64
        self.db=collector.connect(self.database)
        archive.initialize(self.db)
        self.collect()
        self.start()

    def collect(self):
        collector.collect(self.db,str(self.log),'home',time.time(),status={'addon':{'state':'started','version':'0.0.13'}})
        archive.persist(self.db)

    def start(self, options=None):
        self.server=remote_api.LimitedServer(('127.0.0.1',0),remote_api.handler(self.database,'home',self.secret,options))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.url='http://127.0.0.1:'+str(self.server.server_port)

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def tearDown(self):
        self.stop()
        self.db.close()
        self.temp.cleanup()

    def request(self,path='/v1/report',payload=None,key=None,stamp=None,nonce=None):
        body=json.dumps(payload or {},separators=(',',':')).encode()
        stamp=stamp or str(int(time.time()))
        nonce=nonce or secrets.token_hex(16)
        headers={'Content-Type':'application/json','X-Ezville-Time':stamp,'X-Ezville-Nonce':nonce,
            'X-Ezville-Signature':remote_api.sign(key or self.secret,'POST',path,stamp,nonce,body)}
        return urllib.request.Request(self.url+path,body,headers=headers),nonce

    def read(self,path='/v1/report',payload=None):
        request,nonce=self.request(path,payload)
        with urllib.request.urlopen(request) as response:
            body=response.read()
            signed=remote_api.sign(self.secret,'RESPONSE:200',path,response.headers['X-Ezville-Time'],nonce,body)
            self.assertEqual(response.headers['X-Ezville-Signature'],signed)
            self.assertEqual(response.headers['Cache-Control'],'no-store')
            return json.loads(body)

    def status(self,request):
        try:
            urllib.request.urlopen(request).close()
            return 200
        except urllib.error.HTTPError as error:
            return error.code

    def test_no_ha_login_and_real_source_append(self):
        first=self.read('/v1/health')
        self.assertEqual(first['log_line_count'],1)
        with self.log.open('a') as stream:
            stream.write('[MONITOR_TEST] real source two\n')
        self.collect()
        second=self.read('/v1/health')
        self.assertGreater(second['batch_count'],first['batch_count'])
        self.assertEqual(second['log_line_count'],2)
        self.assertTrue(second['collector_receiving'])
        latest=self.read('/v1/recent')
        self.assertIn('real source two',latest['events'][0]['text'])

    def test_heartbeat_not_mistaken_for_new_log(self):
        first=self.read('/v1/health')
        self.collect()
        second=self.read('/v1/health')
        self.assertGreater(second['batch_count'],first['batch_count'])
        self.assertEqual(second['log_line_count'],first['log_line_count'])

    def test_no_key_wrong_key_expired_replay_and_path_tampering(self):
        self.assertEqual(self.status(urllib.request.Request(self.url+'/v1/report',b'{}',headers={'Cookie':'ha-session=anything'})),401)
        request,_=self.request(key='wrong')
        self.assertEqual(self.status(request),401)
        request,_=self.request(stamp=str(int(time.time())-61))
        self.assertEqual(self.status(request),401)
        request,_=self.request()
        self.assertEqual(self.status(request),200)
        self.assertEqual(self.status(request),401)
        request,_=self.request('/v1/health')
        request.full_url=self.url+'/v1/report'
        self.assertEqual(self.status(request),401)

    def test_readonly_allowlist_and_limits(self):
        for path in ['/v1/delete','/v1/report?days=90','/v1/analysis','/restart']:
            request,_=self.request(path)
            self.assertEqual(self.status(request),404)
        request,_=self.request('/v1/report',{'sql':'DELETE FROM batches'})
        self.assertEqual(self.status(request),400)
        request,_=self.request('/v1/recent',{'limit':31})
        self.assertEqual(self.status(request),400)
        self.assertEqual(self.status(urllib.request.Request(self.url+'/v1/report')),404)
        self.assertEqual(self.db.execute('SELECT count(*) FROM batches').fetchone()[0],1)

    def test_archive_and_cursor_survive_server_and_database_restart(self):
        first=self.read('/v1/health')
        self.stop()
        self.db.close()
        self.db=collector.connect(self.database)
        self.start()
        self.collect()
        second=self.read('/v1/health')
        self.assertEqual(second['log_line_count'],first['log_line_count'])
        self.assertGreater(second['batch_count'],first['batch_count'])

    def test_recent_redacts_private_values(self):
        with self.log.open('a') as stream:
            stream.write('[ERROR] password=privatevalue host 192.168.0.167\n')
        self.collect()
        result=json.dumps(self.read('/v1/recent'))
        self.assertNotIn('privatevalue',result)
        self.assertNotIn('192.168.0.167',result)

    def test_packet_samples_never_leave_remote_recent_api(self):
        with self.log.open('a') as stream:
            stream.write('[PACKET_PROFILE] {"samples":["F799010200AABB"]}\n')
            stream.write('[SIGNAL] receved: F799010200AABB\n')
            stream.write('[INFO] status is available\n')
        self.collect()
        response=json.dumps(self.read('/v1/recent',{'limit':30}))
        self.assertNotIn('F799010200AABB',response)
        self.assertIn('status is available',response)

    def test_production_client_validates_signed_response_without_ha_auth(self):
        local_opener=urllib.request.build_opener()
        server_origin=self.url
        class Proxy:
            def open(self,request,timeout):
                self.assert_no_cookie = 'Cookie' not in request.headers
                request.full_url=server_origin + urllib.parse.urlsplit(request.full_url).path
                return local_opener.open(request,timeout=timeout)
        proxy=Proxy()
        with patch.object(read_remote.urllib.request,'build_opener',return_value=proxy):
            result=read_remote.query('https://monitor.example.test',self.secret,'health')
        self.assertTrue(proxy.assert_no_cookie)
        self.assertEqual(result['log_line_count'],1)

    def test_client_rejects_insecure_origins(self):
        for origin in ['http://host','https://user:pass@host','https://host/path','https://host?token=x','https://host/#secret']:
            with self.assertRaises(ValueError):
                read_remote.query(origin,self.secret,'report')

    def test_rate_limit(self):
        guard=remote_api.Guard()
        for _ in range(60):
            self.assertTrue(guard.allow(1000))
        self.assertFalse(guard.allow(1000))
        self.assertTrue(guard.allow(1061))

    def test_daily_reports_disabled_or_unknown_without_claiming_delivery(self):
        result = self.read('/v1/daily')
        self.assertIsNone(result['daily_enabled'])
        self.assertIsNone(result['latest_delivery'])
        self.assertEqual(result['summary_source'], 'preview')
        disabled = remote_api.daily_snapshot(self.database, 'home', {'daily_enabled': False}, time.time())
        self.assertFalse(disabled['daily_enabled'])

    def test_daily_preserves_saved_new_patterns_and_removes_private_samples(self):
        now = time.time()
        day, start, end = daily.window(now)
        summary = daily.summarize(self.database, 'home', start, end)
        summary['packet_patterns'] = [{'signature': '99:01:02:00', 'known': False, 'count': 3,
                                     'first': start + 1, 'last': start + 2, 'samples': ['F799010200AABB']}]
        summary['new_packet_patterns'] = ['99:01:02:00']
        summary['packet_markers'] = [{'at': start + 1, 'label': 'bell', 'candidates': [
            {'signature': '99:01:02:00', 'before': 0, 'after': 3, 'sample': 'F799010200AABB'}]}]
        summary['calls'] = [{'at': start + 2, 'request_id': 'private-call-id'}]
        daily.initialize(self.db)
        self.db.execute('INSERT INTO daily_state VALUES (?,?)', ('enabled_since', start - 1))
        self.db.execute('INSERT INTO daily_reports VALUES (?,?,?,?)', (day, 'done', end + 100, json.dumps(summary)))
        self.db.commit()
        result = self.read('/v1/daily')
        self.assertEqual(result['summary_source'], 'saved_report')
        self.assertEqual(result['summary']['new_packet_patterns'], ['99:01:02:00'])
        self.assertEqual(result['latest_delivery']['status'], 'done')
        self.assertNotIn('F799010200AABB', json.dumps(result))
        self.assertNotIn('private-call-id', json.dumps(result))
        self.assertEqual(result['summary']['packet_patterns'][0]['sample_byte_lengths'], [7])

    def test_daily_requires_signature_and_rejects_write_payloads(self):
        request, _ = self.request('/v1/daily', key='wrong')
        self.assertEqual(self.status(request), 401)
        request, _ = self.request('/v1/daily', {'daily_enabled': True})
        self.assertEqual(self.status(request), 400)

    def test_manual_action_uses_separate_key_and_rejects_replay(self):
        self.stop()
        action_key = 'a' * 64
        self.start({'daily_enabled': True, 'node_id': 'home', 'remote_action_secret': action_key})
        path = remote_api.ACTION_PATH
        read_request, _ = self.request(path)
        self.assertEqual(self.status(read_request), 401)
        self.assertNotIn(path, read_remote.PATHS)
        stamp, nonce, body = str(int(time.time())), secrets.token_hex(16), b'{}'
        headers = {'Content-Type': 'application/json', 'X-Ezville-Time': stamp, 'X-Ezville-Nonce': nonce,
                   'X-Ezville-Signature': remote_api.sign_action(action_key, 'POST', path, stamp, nonce, body)}
        request = urllib.request.Request(self.url + path, body, headers=headers)
        with patch.dict('os.environ', {'SUPERVISOR_TOKEN': 'test'}), patch.object(daily, 'run_daily', return_value=True) as run:
            with urllib.request.urlopen(request) as response:
                result = json.loads(response.read())
                self.assertTrue(result['ran'])
            self.assertEqual(self.status(request), 401)
            run.assert_called_once()
            self.assertTrue(run.call_args.kwargs['force'])

    def test_manual_action_is_disabled_if_action_key_equals_read_key(self):
        self.stop()
        self.start({'daily_enabled': True, 'node_id': 'home', 'remote_action_secret': self.secret})
        request, _ = self.request(remote_api.ACTION_PATH)
        self.assertEqual(self.status(request), 404)

    def test_manual_action_client_verifies_signed_response(self):
        self.stop()
        action_key = 'a' * 64
        self.start({'daily_enabled': True, 'node_id': 'home', 'remote_action_secret': action_key})
        local_opener = urllib.request.build_opener()
        server_origin = self.url
        class Proxy:
            def open(self, request, timeout):
                request.full_url = server_origin + urllib.parse.urlsplit(request.full_url).path
                return local_opener.open(request, timeout=timeout)
        with patch.dict('os.environ', {'SUPERVISOR_TOKEN': 'test'}), patch.object(daily, 'run_daily', return_value=True), \
             patch.object(run_daily_remote.urllib.request, 'build_opener', return_value=Proxy()):
            status, result = run_daily_remote.run('https://monitor.example.test', action_key)
        self.assertEqual(status, 200)
        self.assertTrue(result['ran'])
        with self.assertRaises(ValueError):
            run_daily_remote.run('http://monitor.example.test', action_key)

    def test_ha_ingress_button_requires_csrf_and_runs_manual_report(self):
        options = {'daily_enabled': True, 'node_id': 'home'}
        handler = archive.handler(self.database, 'home', options)
        handler.allowed = lambda self: True  # Simulate the Supervisor ingress peer.
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = 'http://127.0.0.1:' + str(server.server_port)
        try:
            with urllib.request.urlopen(url + '/') as response:
                page = response.read().decode()
                self.assertIn('action="daily/run"', page)
                token = re.search(r'name="csrf" value="([^"]+)"', page).group(1)
            wrong = urllib.request.Request(url + '/daily/run', b'csrf=wrong',
                headers={'Content-Type': 'application/x-www-form-urlencoded'})
            self.assertEqual(self.status(wrong), 403)
            request = urllib.request.Request(url + '/daily/run', ('csrf=' + token).encode(),
                headers={'Content-Type': 'application/x-www-form-urlencoded'})
            with patch.dict('os.environ', {'SUPERVISOR_TOKEN': 'test'}), patch.object(daily, 'run_daily', return_value=True) as run:
                with urllib.request.urlopen(request) as response:
                    self.assertIn('다시 생성', response.read().decode())
                run.assert_called_once()
                self.assertTrue(run.call_args.kwargs['force'])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_daily_limits_discovery_and_reports_coverage(self):
        now = time.time()
        day, start, end = daily.window(now)
        summary = daily.summarize(self.database, 'home', start, end)
        summary['packet_patterns'] = [{'signature': '99:01:02:%02X' % i, 'known': False,
            'count': i + 1, 'first': start, 'last': start, 'samples': []} for i in range(130)]
        summary['new_packet_patterns'] = [item['signature'] for item in summary['packet_patterns']]
        daily.initialize(self.db)
        self.db.execute('INSERT INTO daily_reports VALUES (?,?,?,?)', (day, 'done', end + 100, json.dumps(summary)))
        self.db.commit()
        result = self.read('/v1/daily')
        self.assertTrue(result['truncated'])
        self.assertEqual(result['totals_before_limits']['packet_patterns'], 130)
        self.assertEqual(len(result['summary']['packet_patterns']), 128)
        self.assertEqual(result['summary']['packet_patterns'][0]['count'], 130)

    def test_request_body_and_method_bound_to_signature(self):
        stamp=str(int(time.time()))
        nonce='0'*32
        signature=remote_api.sign(self.secret,'POST','/v1/report',stamp,nonce,b'{}')
        guard=remote_api.Guard()
        self.assertFalse(guard.authenticate(self.secret,'/v1/report',stamp,nonce,b'{"x":1}',signature,time.time()))
        self.assertNotEqual(signature,remote_api.sign(self.secret,'GET','/v1/report',stamp,nonce,b'{}'))

if __name__=='__main__':
    unittest.main()
