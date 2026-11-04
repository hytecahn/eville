import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ezville_monitor'))
import moes_quality as m


class Stop:
    def is_set(self): return False
    def wait(self, seconds): pass


class FakeSession:
    retained = {'bridge/info': {'version': 'test-fixture'}}
    calls = []
    closed = False
    def __enter__(self): return self
    def __exit__(self, *args): self.closed = True
    def request(self, ieee, attempt):
        self.calls.append((ieee, attempt))
        return {'ieee': ieee, 'attempt': attempt, 'status': 'success', 'rtt_ms': 1000, 'value': 3}


class QualityTests(unittest.TestCase):
    def response(self):
        return {'transaction': 'abc', 'status': 'ok', 'data': {'address': 123, 'endpoint': 1,
            'clusterID': 0, 'header': {'commandIdentifier': 1, 'transactionSequenceNumber': 9},
            'data': {'type': 'Buffer', 'data': [24, 9, 1, 0, 0, 0, 32, 3]}}}

    def test_valid_response(self):
        self.assertEqual(m.validate(self.response(), 'abc', 123, 9), 3)

    def test_reject_wrong_transaction_address_endpoint_cluster_and_command(self):
        changes = [('transaction', 'other'), ('address', 124), ('endpoint', 2), ('clusterID', 1),
                   ('commandIdentifier', 0), ('transactionSequenceNumber', 10)]
        for key, value in changes:
            with self.subTest(key=key):
                r = self.response()
                target = r if key == 'transaction' else r['data']['header'] if key in ('commandIdentifier', 'transactionSequenceNumber') else r['data']
                target[key] = value
                with self.assertRaises(ValueError): m.validate(r, 'abc', 123, 9)

    def test_reject_wrong_attr_status_type_direction_and_malformed_bytes(self):
        for index, value in [(0, 0), (1, 8), (2, 0), (3, 1), (5, 1), (6, 33), (7, 256)]:
            with self.subTest(index=index):
                r = self.response(); r['data']['data']['data'][index] = value
                with self.assertRaises(ValueError): m.validate(r, 'abc', 123, 9)

    def test_request_is_read_only_and_rejects_other_devices(self):
        p = m.read_request(next(iter(m.TARGETS)), 123, 'abc', 9)['params']
        self.assertEqual((p['cluster_key'], p['dst_endpoint'], p['timeout']), (0, 1, 8000))
        self.assertEqual(p['zcl']['command_key'], 'read')
        self.assertEqual(p['zcl']['payload'], [{'attrId': 0}])
        for nwk in [0, 65535, -1, True]:
            with self.assertRaises(ValueError): m.read_request(next(iter(m.TARGETS)), nwk, 'abc', 9)
        with self.assertRaises(ValueError): m.read_request(m.ROUTER, 123, 'abc', 9)

    def test_baseline_stats_and_missing_not_zero_success(self):
        raw = [1144,885,871,853,866,874,856,906,849,856,850,830,847,833,829,838,830,873,817,813]
        s = m.summarize([{'status':'success','rtt_ms':n} for n in raw])
        self.assertEqual((s['median_ms'], s['p95_ms']), (851.5, 906))
        self.assertIsNone(m.summarize([])['success_pct'])

    def test_stale_neighbor_values_never_claim_current(self):
        ieee = next(iter(m.TARGETS)); now = 1800000000
        s = {'checkedAt':m.iso(now), 'reports':{ieee:{'checkedAt':m.iso(now),
             'observedParent':m.ROUTER, 'routerNeighbors':[{'eui64':ieee,'relationship':1,'deviceType':2,'lqi':18}]}}}
        self.assertEqual(m.parent_snapshot(s,now)['devices'][ieee]['mg24_lqi'],18)
        self.assertIsNone(m.parent_snapshot(s,now+601)['devices'][ieee]['mg24_lqi'])
        self.assertIsNone(m.parent_snapshot(s,now-1)['devices'][ieee]['parent_ieee'])

    def test_alternation_durable_records_duplicate_slot_and_export(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td)/'test.sqlite'); session = FakeSession(); session.calls=[]
            self.assertTrue(m.run(db,'slot1',Stop(),lambda:session))
            self.assertTrue(session.closed)
            self.assertEqual(len(session.calls),40)
            self.assertEqual(session.calls[:4],[(i,n) for n in (1,2) for i in m.TARGETS])
            self.assertFalse(m.run(db,'slot1',Stop(),lambda:session))
            out = m.snapshot(db,{'moes_quality_enabled':True})
            self.assertEqual(len(out['runs'][0]['attempts']),40)
            self.assertEqual(out['runs'][0]['status'],'complete')
            self.assertEqual(m.snapshot(db,{'moes_quality_enabled':True},'absent')['runs'],[])

    def test_failure_keeps_partial_attempts_and_closes_subscriber(self):
        with tempfile.TemporaryDirectory() as td:
            db=str(Path(td)/'test.sqlite'); session=FakeSession()
            original=session.request
            def request(i,n):
                if n == 2: raise RuntimeError('secret must not persist')
                return original(i,n)
            session.request=request
            m.run(db,'slot2',Stop(),lambda:session)
            out=m.snapshot(db,{'moes_quality_enabled':True})
            self.assertEqual(out['runs'][0]['status'],'incomplete')
            self.assertEqual(len(out['runs'][0]['attempts']),2)
            self.assertNotIn('secret must not persist',json.dumps(out))
            self.assertTrue(session.closed)

    def test_kst_slot(self):
        now=m.datetime.fromisoformat('2026-11-05T06:01:00+09:00').timestamp()
        self.assertEqual(m.slot(now),'2026-11-05T06:00:00+09:00')

    def test_disabled_worker_does_not_open_mqtt(self):
        with patch.object(m,'credentials') as creds:
            m.worker('unused',{},Stop()); creds.assert_not_called()


class CommissioningTests(unittest.TestCase):
    def test_explicit_verification_runs_once_across_restarts(self):
        class Stopped(Stop):
            calls = 0
            def is_set(self):
                self.calls += 1
                return self.calls > 40
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td)/'test.sqlite')
            session = FakeSession(); session.calls = []
            options = {'moes_quality_enabled': True, 'moes_quality_verification_id': 'install-1'}
            with patch.object(m, 'Session', return_value=session), patch.object(m, 'credentials', return_value={}), patch.dict(m.os.environ, {'SUPERVISOR_TOKEN':'test'}):
                stop = Stopped()
                m.worker(db, options, stop)
                m.worker(db, options, stop)
            out = m.snapshot(db, options)
            self.assertEqual(len(out['runs']), 1)
            self.assertEqual(out['runs'][0]['slot'], 'verification:install-1')
            self.assertEqual(out['runs'][0]['detail']['kind'], 'verification')
            self.assertEqual(out['runs'][0]['status'], 'complete')
            self.assertEqual(len(session.calls), 40)

    def test_invalid_or_empty_verification_id_does_not_run(self):
        class Stopped(Stop):
            def is_set(self): return True
        for value in ['', '../bad', 3, 'a'*65]:
            with tempfile.TemporaryDirectory() as td, patch.object(m, 'run') as run:
                m.worker(str(Path(td)/'test.sqlite'), {'moes_quality_enabled':True, 'moes_quality_verification_id':value}, Stopped())
                run.assert_not_called()

if __name__ == '__main__': unittest.main()

