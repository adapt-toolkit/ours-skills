import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
from self_reminders import Store, instant, load_config


class Reminders(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.root.chmod(0o700)
        self.store = Store(self.root / 'reminders.sqlite3')
        self.request = {'request_key': 'actual-request', 'title': 'Check work',
                        'local_time': '2030-01-01T00:00:00', 'timezone': 'UTC'}
        self.due = instant(self.request['local_time'], 'UTC')

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def create(self, **kwargs):
        return self.store.create({**self.request, **kwargs}, now=self.due - 30)

    def admit(self, **kwargs):
        self.create(**kwargs)
        return self.store.tick(lambda _: {'status': 'queued', 'prompt_id': 'receipt'}, now=self.due)[0]['event_id']

    def test_idle_and_one_due_admission_without_destination(self):
        calls = []
        r = self.create()
        adapter = lambda e: calls.append(e) or {'status': 'queued', 'prompt_id': 'receipt'}
        self.store.tick(adapter, self.due - 1)
        self.assertEqual(calls, [])
        self.store.tick(adapter, self.due)
        self.store.tick(adapter, self.due + 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(set(calls[0]), {'event_id', 'revision'})
        v = self.store.inspect(calls[0]['event_id'], self.due + 1)
        self.assertEqual(v['reminder']['id'], r['id'])
        result = self.store.complete({'event_id': calls[0]['event_id'], 'expected_revision': 1}, self.due + 1)
        self.assertEqual(result['state'], 'handled')
        self.assertEqual(self.store.complete({'event_id': calls[0]['event_id'], 'expected_revision': 1}, self.due + 2), result)

    def test_idempotency_and_conflict(self):
        r = self.create()
        self.assertEqual(self.create()['id'], r['id'])
        with self.assertRaises(ValueError):
            self.create(title='Different meaning')
        for k in ('contact_cid', 'route', 'destination'):
            with self.assertRaises(ValueError):
                self.create(**{k: 'not accepted'})

    def test_queued_cancel_and_revision_are_checked_at_handling(self):
        eid = self.admit()
        rid = self.store.inspect(eid, self.due)['reminder']['id']
        self.store.change({'id': rid, 'expected_revision': 1}, cancel=True)
        self.assertFalse(self.store.inspect(eid, self.due)['eligible'])
        with self.assertRaises(ValueError):
            self.store.complete({'event_id': eid, 'expected_revision': 1}, self.due)

    def test_update_invalidates_original_queued_event(self):
        eid = self.admit()
        rid = self.store.inspect(eid, self.due)['reminder']['id']
        self.store.change({'id': rid, 'expected_revision': 1, 'local_time': '2030-01-02T00:00:00'}, now=self.due)
        self.assertFalse(self.store.inspect(eid, self.due)['eligible'])
        with self.assertRaises(ValueError):
            self.store.change({'id': rid, 'expected_revision': 1}, cancel=True)

    def test_reboot_does_not_replay_unknown_or_queued(self):
        eid = self.admit()
        self.store.db.execute("UPDATE self_events SET state='submitting' WHERE id=?", (eid,))
        self.store.db.close()
        self.store = Store(self.root / 'reminders.sqlite3')
        self.store.recover()
        self.assertEqual(self.store.inspect(eid, self.due)['event']['state'], 'uncertain')
        calls = []
        self.store.tick(lambda x: calls.append(x), self.due + 1)
        self.assertEqual(calls, [])

    def test_expired_reminders_do_not_wake_after_downtime(self):
        self.create(late_window_seconds=10)
        calls = []
        self.store.tick(lambda x: calls.append(x), self.due + 11)
        self.assertEqual(calls, [])

    def test_only_definite_unavailability_retries_three_times(self):
        self.create()
        calls = []
        adapter = lambda e: calls.append(e) or {'status': 'unavailable'}
        for t in (self.due, self.due + 1, self.due + 30, self.due + 89, self.due + 90, self.due + 500):
            self.store.tick(adapter, t)
        self.assertEqual(len(calls), 3)
        self.assertEqual(self.store.inspect(calls[0]['event_id'], self.due)['event']['state'], 'failed')

    def test_dst_gap_and_fold(self):
        with self.assertRaises(ValueError):
            instant('2030-03-10T02:30:00', 'America/New_York')
        with self.assertRaises(ValueError):
            instant('2030-11-03T01:30:00', 'America/New_York')
        self.assertEqual(instant('2030-11-03T01:30:00', 'America/New_York', 1) -
                         instant('2030-11-03T01:30:00', 'America/New_York', 0), 3600)

    def test_setup_and_real_adapter_boundary_without_messaging(self):
        agent = self.root / 'agent'
        (agent / '.ours-bridge').mkdir(parents=True)
        cid = 'A' * 64
        (agent / '.ours-bridge/descriptor.json').write_text(json.dumps({'role': 'TestAgent', 'cid': cid}))
        module = self.root / 'control.mjs'
        captured = self.root / 'captured.json'
        module.write_text('import{writeFileSync}from"node:fs";export async function controlRequest(dir,r){'
                          'writeFileSync(' + json.dumps(str(captured)) + ',JSON.stringify(r));'
                          'return{ok:true,result:{state:"queued",promptId:"fixture-receipt"}}}')
        private = self.root / 'state'
        setup = subprocess.run([sys.executable, str(SCRIPTS / 'setup.py'), '--agent-state-dir', str(agent),
                                '--fleet-control-module', str(module), '--state-dir', str(private)],
                               capture_output=True, text=True, check=True)
        cfg_path = Path(json.loads(setup.stdout)['config'])
        cfg = load_config(cfg_path)
        self.assertNotIn('route', cfg)
        def cli(action, data=None):
            r = subprocess.run([sys.executable, str(SCRIPTS / 'self_reminders.py'), '--config', str(cfg_path), action],
                               input=json.dumps(data or {}), text=True, capture_output=True, check=True)
            return json.loads(r.stdout)
        self.assertFalse(cli('check')['outbound_destination_required'])
        self.assertFalse(Path(cfg['database']).exists())
        future = datetime.fromtimestamp(time.time() + .2, timezone.utc).replace(tzinfo=None).isoformat()
        cli('create', {'request_key': 'cli-roundtrip', 'title': 'Internal check', 'local_time': future, 'timezone': 'UTC'})
        time.sleep(.25)
        admitted = cli('tick')
        self.assertEqual(admitted[0]['state'], 'queued')
        view = cli('inspect', {'event_id': admitted[0]['event_id']})
        self.assertTrue(view['eligible'])
        self.assertEqual(cli('complete', {'event_id': admitted[0]['event_id'], 'expected_revision': 1})['state'], 'handled')
        self.assertEqual(cli('tick'), [])
        payload = {'config': cfg, 'config_path': str(cfg_path), 'event': {'event_id': 'example-event', 'revision': 1}}
        run = subprocess.run(['node', str(SCRIPTS / 'fleet_wake.mjs')], input=json.dumps(payload),
                             text=True, capture_output=True, check=True)
        self.assertEqual(json.loads(run.stdout)['status'], 'queued')
        actual = json.loads(captured.read_text())
        self.assertEqual(actual['command'], 'submit_prompt')
        self.assertIn('example-event', actual['text'])
        self.assertIn('does not send any message', actual['text'])
        cfg['expected_agent_cid'] = 'B' * 64
        payload['config'] = cfg
        captured.unlink()
        run = subprocess.run(['node', str(SCRIPTS / 'fleet_wake.mjs')], input=json.dumps(payload),
                             text=True, capture_output=True, check=True)
        self.assertEqual(json.loads(run.stdout)['status'], 'rejected')
        self.assertFalse(captured.exists())


if __name__ == '__main__':
    unittest.main()
