import datetime as dt
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ezville_monitor'))
import archive
import collector
import maintenance
from reports import report


class LocalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.database = self.root / 'telemetry.sqlite'
        self.db = collector.connect(self.database)
        archive.initialize(self.db)
        self.log = self.root / 'log'
        self.log.write_text('[ERROR] failure\n')

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def collect(self, now):
        collector.collect(self.db, str(self.log), 'home', now,
                          status={'addon': {'state':'started','version':'0.0.13'}})
        archive.persist(self.db)

    def test_archive_survives_reopen_and_no_duplicate(self):
        now = time.time()
        self.collect(now)
        self.collect(now + 60)
        self.assertEqual(self.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        self.db.close()
        self.db = collector.connect(self.database)
        result = report(self.database, 'home', now + 60)
        self.assertEqual(result['counts']['error'], 1)
        self.assertEqual(result['latest_status']['addon']['version'], '0.0.13')
        self.assertFalse(result['observation_complete'])

    def test_normal_configuration_is_not_a_timeout_incident(self):
        self.log.write_text('[CONFIG] {"ew11_timeout":600}\n[DEBUG] 상태 정상 (타임아웃:300초)\n[WARNING] EW11 수신 타임아웃\nTimeoutError\n')
        now=time.time()
        self.collect(now)
        self.assertEqual(report(self.database,'home',now)['counts']['timeout'],2)

    def test_complete_window_and_stale(self):
        now = time.time()
        for stamp in range(int(now)-600, int(now)+1, 60):
            self.collect(stamp)
        self.assertTrue(report(self.database,'home',int(now),600/86400)['observation_complete'])
        self.assertFalse(archive.snapshot(self.database,'home',now+181)['collector_receiving'])

    def test_retention(self):
        self.collect(time.time()-31*86400)
        self.assertEqual(self.db.execute('SELECT count(*) FROM batches').fetchone()[0],0)

    def test_monday_seoul_schedule(self):
        before = dt.datetime(2026,10,11,22,59,tzinfo=dt.timezone.utc).timestamp()
        after = dt.datetime(2026,10,11,23,0,tzinfo=dt.timezone.utc).timestamp()
        self.assertIsNone(maintenance.due_week(before))
        self.assertEqual(maintenance.due_week(after),'2026-10-12')

    def test_disabled_no_network(self):
        with patch.object(maintenance,'github') as remote:
            self.assertFalse(maintenance.run_week(self.database, {'weekly_enabled':False}, time.time()))
            remote.assert_not_called()

    def test_bounded_retries_and_interrupted_run(self):
        now = dt.datetime(2026,10,12,9,tzinfo=maintenance.KST).timestamp()
        options = {'weekly_enabled':True,'openai_api_key':'private','node_id':'home'}
        with patch.object(maintenance,'github',side_effect=OSError):
            self.assertTrue(maintenance.run_week(self.database,options,now))
            self.assertFalse(maintenance.run_week(self.database,options,now+1))
            self.assertTrue(maintenance.run_week(self.database,options,now+3601))
            self.assertTrue(maintenance.run_week(self.database,options,now+7202))
            self.assertFalse(maintenance.run_week(self.database,options,now+10803))
        saved = self.db.execute('SELECT body FROM weekly_runs').fetchone()[0]
        self.assertNotIn('private',saved)

    def test_reject_disallowed_syntax_and_large_deletion(self):
        path = maintenance.ALLOWED[0]
        original = 'value = 1\n'*100
        for edits in [[{'path':'../../escape','before':'x','after':'x=1'}],
                      [{'path':path,'before':original,'after':'not valid python!!'}],
                      [{'path':path,'before':original,'after':'x=1'}],
                      [{'path':path,'before':'value = 1','after':'value=2'}]]:
            with self.assertRaises((ValueError,SyntaxError)):
                maintenance.validate_files(edits,{path:original})

    def test_model_output_is_parsed_never_executed(self):
        path = maintenance.ALLOWED[0]
        result=maintenance.validate_files([{'path':path,'before':'value=1','after':'raise RuntimeError("never execute")'}],{path:'value=1\n'})
        self.assertIn(path,result)

    def test_api_store_disabled_and_no_tools(self):
        response = {'status':'completed','output':[{'content':[{'type':'output_text','text':json.dumps({'analysis':'ok','evidence':[],'files':[]})}]}]}
        with patch.object(maintenance,'request_json',return_value=response) as api:
            maintenance.propose({'ai_model':'model','openai_api_key':'private'}, {}, {})
            payload = api.call_args.args[2]
            self.assertFalse(payload['store'])
            self.assertNotIn('tools',payload)

    def test_publication_never_updates_master(self):
        options={'github_token':'private'}
        replies=[{'object':{'sha':'base'}},{'sha':'tree'},{'sha':'commit'}, {}, {'html_url':'pr-url'}]
        from urllib.error import HTTPError
        error=HTTPError('url',404,'missing',{},None)
        with patch.object(maintenance,'github',side_effect=[error]+replies) as api:
            result=maintenance.publish(options,'2026-10-12','base','oldtree',
                {maintenance.CONFIG:'{"version":"0.0.13"}',maintenance.CHANGELOG:'old'}, {maintenance.ALLOWED[0]:'value=2'})
            self.assertEqual(result,'pr-url')
            writes=[call.args for call in api.call_args_list if len(call.args)>2]
            self.assertEqual(writes[-2][2]['ref'],'refs/heads/bot/ezville-weekly-2026-10-12')
            self.assertTrue(writes[-1][2]['draft'])
            self.assertNotIn('raw log',writes[-1][2]['body'].lower())

    def test_weekly_incomplete_window_blocks_publication_and_is_idempotent(self):
        import base64
        now = dt.datetime(2026,10,12,9,tzinfo=maintenance.KST).timestamp()
        options={'weekly_enabled':True,'openai_api_key':'private','github_token':'private',
                 'node_id':'home','publish_pr':True}
        source={path:'value=1\n' for path in maintenance.ALLOWED}
        source[maintenance.CONFIG]='{"version":"0.0.13"}'
        source[maintenance.CHANGELOG]='old'
        def api(token,path,*args):
            if path=='/git/ref/heads/master':
                return {'object':{'sha':'base'}}
            if path=='/git/commits/base':
                return {'tree':{'sha':'tree'}}
            name=path.split('/contents/')[1].split('?')[0]
            return {'encoding':'base64','content':base64.b64encode(source[name].encode()).decode()}
        proposal={'analysis':'diagnosis','evidence':['evidence'],'files':[
            {'path':maintenance.ALLOWED[0],'before':'value=1','after':'value=2'}]}
        summary={'observation_complete':False,'latest_status':{'addon':{'state':'started','version':'0.0.13'}}}
        with patch.object(maintenance,'github',side_effect=api), patch.object(maintenance,'propose',return_value=proposal), \
             patch.object(maintenance,'report',return_value=summary),patch.object(maintenance,'publish') as publish:
            self.assertTrue(maintenance.run_week(self.database,options,now))
            self.assertFalse(maintenance.run_week(self.database,options,now+7200))
            publish.assert_not_called()
        body=json.loads(self.db.execute('SELECT body FROM weekly_runs').fetchone()[0])
        self.assertEqual(body['analysis'],'diagnosis')
        self.assertNotIn('private',json.dumps(body))

if __name__=='__main__':
    unittest.main()
