#!/usr/bin/env python3
"""Synthetic behavioural checks for task_review.py. No real records, routes or sends."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
import threading
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'scripts'))
import preference_tools  # noqa: E402
import source_store as source  # noqa: E402
import task_review as tr  # noqa: E402

ZONE = 'Europe/Prague'
QUIET = {'start': '21:00', 'end': '08:00', 'basis': 'user_confirmed', 'confirmed_wire_id': 'synthetic-wire-q'}
PASS = timedelta(minutes=150)


def utc(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


class User:
    """One synthetic private user: own dir, source, prefs, sidecar and (optional) legacy ledger."""

    def __init__(self, root, name, zone=ZONE, quiet=QUIET):
        self.dir = Path(root) / name
        self.dir.mkdir(mode=0o700)
        os.chmod(self.dir, 0o700)
        self.src = source.connect(self.dir / 'source.sqlite3', initialize=True)
        prefs = self.dir / 'preferences.json'
        prefs.write_text(json.dumps({'timezone': zone, 'timezone_source': {
            'kind': 'user_message', 'wire_id': 'synthetic-tz', 'confirmed_at_utc': '2026-01-01T00:00:00Z'}}))
        os.chmod(prefs, 0o600)
        preference_tools.configure(self.src, prefs, 'user_message')
        self.ns = source.meta(self.src, 'namespace')
        self.review = tr.connect_review(self.dir / 'task-review.sqlite', self.ns, initialize=True)
        self.cfg = {'task_review_quiet_hours': quiet} if quiet else {}
        self.ledger = None

    def close(self):
        for db in (self.src, self.review, self.ledger):
            if db is not None:
                db.close()

    def put(self, tid, title='Synthetic task', prereqs=()):
        rev = self.rev(tid)
        if rev == 0:
            return source.run(self.src, 'task_put', {'id': tid, 'expected_revision': 0, 'record': {
                'id': tid, 'title': title, 'source': 'synthetic', 'status': 'open', 'task_schema_version': 2,
                'prerequisites': list(prereqs)}})
        return source.run(self.src, 'task_put', {'id': tid, 'expected_revision': rev, 'changes': {'title': title}})

    def status(self, tid, status):
        return source.run(self.src, 'task_status', {'id': tid, 'expected_revision': self.rev(tid),
                                                    'status': status, 'source': 'synthetic'})

    def rev(self, tid):
        row = self.src.execute('SELECT revision FROM tasks WHERE id=?', (tid,)).fetchone()
        return 0 if row is None else row[0]

    def call(self, action, at, **args):
        return tr.run(self.review, self.src, {'action': action, 'args': args}, at=at, cfg=self.cfg, ledger=self.ledger)

    def plan(self, at):
        return self.call('plan', at)

    def eligible(self, at):
        return {e['id']: e for e in self.plan(at)['eligible']}

    def claim(self, tid, at, attempt, e=None):
        e = e or self.eligible(at)[tid]
        return self.call('begin_send', at, task_id=tid, attempt_id=attempt, source_revision=e['revision'],
                         claim_token=e['claim_token'])

    def send(self, tid, at, attempt):
        claim = self.claim(tid, at, attempt)
        assert claim['status'] == 'claimed', claim
        return self.call('send_result', at, attempt_id=attempt, outcome='sent', wire_id='synthetic-wire-' + attempt)

    def task(self, tid, at):
        return self.call('status', at, task_id=tid)['task']

    def run_pass(self, at, decide):
        """One periodic pass: decide(e) -> 'send' or 'no_message'. Returns ids sent."""
        sent, quiet = [], []
        for e in self.plan(at)['eligible']:
            if decide(e) == 'send' and e['send_blocked'] is None:
                try:
                    self.send(e['id'], at, '%s@%s' % (e['id'], tr.iso(at)))
                    sent.append(e['id'])
                    continue
                except ValueError as exc:  # e.g. window send budget used: truthful deferral
                    if 'budget' not in str(exc):
                        raise
                    quiet.append({'task_id': e['id'], 'attempt_id': 'd%s@%s' % (e['id'], tr.iso(at)), 'outcome': 'deferred'})
            else:
                quiet.append({'task_id': e['id'], 'attempt_id': 'r%s@%s' % (e['id'], tr.iso(at)), 'outcome': 'no_message'})
        if quiet:
            self.call('record', at, decisions=quiet)
        return sent


class TaskReviewTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.chmod(self.tmp.name, 0o700)
        self.u = User(self.tmp.name, 'alpha')
        self.t0 = utc('2027-03-01T09:00:00')  # 10:00 Prague

    def tearDown(self):
        self.u.close(); self.tmp.cleanup()

    def test_silent_task_is_reconsidered_every_pass_but_not_re_asked(self):
        u = self.u; u.put('t1')
        self.assertIn('t1', u.eligible(self.t0))            # reviewed on the first pass
        u.send('t1', self.t0, 'a1')
        sends, reviewed = [], 0
        at = self.t0
        for _ in range(60):                                   # ~6 days of 150-minute passes
            at += PASS
            e = u.eligible(at).get('t1')
            if e is None:
                continue
            reviewed += 1
            if e['send_blocked'] is None:
                u.send('t1', at, 'a@' + tr.iso(at)); sends.append(at)
            else:
                u.call('record', at, task_id='t1', attempt_id='n@' + tr.iso(at), outcome='no_message')
        self.assertGreater(reviewed, 30)                     # reconsideration cadence ~2-3h
        gaps = [b - a for a, b in zip([self.t0] + sends, sends)]
        self.assertTrue(all(g >= timedelta(days=1) for g in gaps), gaps)  # never the same question every 2h
        self.assertEqual(len(sends), 2)                      # 3 unanswered total, then stop

    def test_exhausted_budget_never_nudges_until_meaningful_change_or_reply(self):
        u = self.u; u.put('t1')
        at = self.t0
        for _ in range(400):                                 # ~6 weeks
            u.run_pass(at, lambda e: 'send'); at += PASS
        self.assertEqual(u.task('t1', at)['total_sends'], 3)
        at = datetime.combine(at.date() + timedelta(days=1), datetime.min.time(), timezone.utc) + timedelta(hours=9)
        self.assertEqual(u.eligible(at)['t1']['send_blocked'], 'repeat_budget_exhausted_unchanged')
        u.put('t1', 'Synthetic task with real new information')  # meaningful change re-allows ONE send
        u.plan(at)
        at += timedelta(hours=1)
        e = u.eligible(at)['t1']
        self.assertIsNone(e['send_blocked'])
        u.send('t1', at, 'after-change')
        at += timedelta(days=30)
        e = u.eligible(at)['t1']
        self.assertEqual(e['send_blocked'], 'repeat_budget_exhausted_unchanged')
        u.call('responded', at, task_id='t1', wire_id='synthetic-in', incoming_utc=tr.iso(at))
        self.assertIsNone(u.eligible(at + PASS)['t1']['send_blocked'])

    def test_source_change_and_old_replies_never_replenish_budget(self):
        u = self.u; u.put('t1'); u.plan(self.t0)
        u.send('t1', self.t0, 'c1')
        u.put('t1', 'Synthetic task renamed')               # change brings review forward
        u.plan(self.t0 + timedelta(minutes=10))
        later = self.t0 + timedelta(hours=1)
        e = u.eligible(later)['t1']
        self.assertTrue(e['send_blocked'].startswith('send_cooldown'))
        with self.assertRaisesRegex(ValueError, 'cooldown'):
            u.claim('t1', later, 'c2', e)
        old = tr.iso(self.t0 - timedelta(minutes=5))         # message older than the review message
        self.assertEqual(u.call('responded', later, task_id='t1', wire_id='old-in', incoming_utc=old)['status'],
                         'recorded_no_reset')
        self.assertEqual(u.task('t1', later)['unanswered_sends'], 1)
        u.call('responded', later, task_id='t1', wire_id='new-in', incoming_utc=tr.iso(later))
        self.assertEqual(u.task('t1', later)['unanswered_sends'], 0)
        nxt = later + timedelta(days=2)
        u.send('t1', nxt, 'c3')
        self.assertEqual(u.call('responded', nxt, task_id='t1', wire_id='new-in', incoming_utc=tr.iso(later))['status'],
                         'already_recorded')                 # replay never resets the later budget
        self.assertEqual(u.task('t1', nxt)['unanswered_sends'], 1)

    def test_completion_cancellation_and_opt_out_stop_returns(self):
        u = self.u
        for t in ('done', 'gone', 'quiet'):
            u.put(t)
        u.plan(self.t0)
        u.status('done', 'completed'); u.status('gone', 'cancelled')
        e = u.eligible(self.t0 + PASS)
        self.assertEqual(set(e), {'quiet'})
        u.call('record', self.t0 + PASS, task_id='quiet', attempt_id='o1', outcome='opt_out')
        p = u.plan(self.t0 + timedelta(days=90))
        self.assertEqual((p['eligible'], p['action']), ([], 'end_turn_quietly'))
        source.run(u.src, 'task_status', {'id': 'done', 'expected_revision': u.rev('done'), 'status': 'open', 'source': 'synthetic'})
        self.assertIn('done', u.eligible(self.t0 + timedelta(days=90, hours=1)))  # explicit reopen resumes

    def test_restart_replay_and_uncertain_send(self):
        u = self.u; u.put('t1')
        e = u.eligible(self.t0)['t1']
        self.assertTrue(u.claim('t1', self.t0, 'u1', e)['send'])
        u.review.close(); u.review = tr.connect_review(u.dir / 'task-review.sqlite', u.ns)   # crash/restart
        self.assertFalse(u.claim('t1', self.t0, 'u1', e)['send'])                          # replay: no
        with self.assertRaisesRegex(ValueError, 'in flight|stale'):
            u.claim('t1', self.t0, 'u2', e)                                                # new attempt: no
        later = self.t0 + timedelta(days=3)
        self.assertEqual(u.plan(later)['held'][0]['reason'], 'uncertain_send_reconcile_first')
        u.call('send_result', later, attempt_id='u1', outcome='uncertain')
        with self.assertRaises(ValueError):
            u.call('record', later, task_id='t1', attempt_id='x', outcome='no_message')
        u.call('send_result', later, attempt_id='u1', outcome='sent', wire_id='synthetic-found')
        row = u.task('t1', later)
        self.assertEqual((row['unanswered_sends'], row['total_sends'], row['in_flight_attempt']), (1, 1, None))
        self.assertEqual(u.call('send_result', later, attempt_id='u1', outcome='sent', wire_id='x')['status'], 'already_recorded')
        self.assertEqual(u.task('t1', later)['total_sends'], 1)
        with self.assertRaises(ValueError):
            u.call('send_result', later, attempt_id='never-claimed', outcome='sent', wire_id='w')

    def test_quiet_hours_unconfirmed_hold_and_downtime_is_bounded(self):
        u = self.u
        for i in range(30):
            u.put('t%d' % i)
        night = utc('2027-03-03T22:30:00')                   # 23:30 Prague
        p = u.plan(night)
        self.assertTrue(p['quiet_now']); self.assertEqual(p['eligible'], [])
        late = utc('2027-03-10T10:00:00')                    # first pass after a week of downtime
        p = u.plan(late)
        self.assertEqual(len(p['eligible']), 20); self.assertEqual(p['eligible_next_pass'], 10)
        sent = u.run_pass(late, lambda e: 'send')
        self.assertEqual(len(sent), 3)                       # burst bound across all tasks
        v = User(self.tmp.name, 'beta', quiet=None)
        v.put('x')
        e = v.eligible(self.t0)['x']
        self.assertFalse(v.plan(self.t0)['sends_possible'])
        with self.assertRaisesRegex(ValueError, 'quiet hours not confirmed'):
            v.claim('x', self.t0, 'q', e)
        v.close()

    def test_concurrent_claims_respect_global_cap(self):
        u = self.u
        for i in range(6):
            u.put('c%d' % i)
        es = u.plan(self.t0)['eligible']
        results = []

        def worker(e):
            db = tr.connect_review(u.dir / 'task-review.sqlite', u.ns)
            src = source.connect(u.dir / 'source.sqlite3', readonly=True)
            try:
                results.append(tr.run(db, src, {'action': 'begin_send', 'args': {
                    'task_id': e['id'], 'attempt_id': 'k' + e['id'], 'source_revision': e['revision'],
                    'claim_token': e['claim_token']}}, at=self.t0, cfg=u.cfg)['status'])
            except (ValueError, sqlite3.OperationalError) as exc:
                results.append(str(exc))
            finally:
                db.close(); src.close()
        threads = [threading.Thread(target=worker, args=(e,)) for e in es]
        [t.start() for t in threads]; [t.join() for t in threads]
        self.assertEqual(results.count('claimed'), 3, results)

    def test_household_tasks_fail_closed_and_recheck_source_after_guard(self):
        u = self.u; u.put('hh-item1')
        e = u.eligible(self.t0)['hh-item1']
        self.assertTrue(e['household_guard_required'])
        with self.assertRaisesRegex(ValueError, 'household'):
            u.claim('hh-item1', self.t0, 'h1', e)
        helper = Path(self.tmp.name) / 'fake_household.py'
        helper.write_text('import sys,json\naction=sys.argv[-1]\n'
                          'print(json.dumps({"status":"bound"} if action=="bind_reminder" else '
                          '{"status":"ok","deliver_allowed":False,"reason":"stale"}))\n')
        u.cfg |= {'household_helper': str(helper), 'household_config': '/nonexistent'}
        with self.assertRaisesRegex(ValueError, 'stale'):
            u.claim('hh-item1', self.t0, 'h2', e)
        # guard passes but the task is cancelled in source while the guard runs
        script = ('import sys,json,sqlite3\naction=sys.argv[-1]\n'
                  'if action=="due_guard":\n'
                  '    sys.path.insert(0,%r)\n    import source_store as s\n'
                  '    db=s.connect(__import__("pathlib").Path(%r))\n'
                  '    r=db.execute("SELECT revision FROM tasks WHERE id=?",("hh-item1",)).fetchone()[0]\n'
                  '    s.run(db,"task_status",{"id":"hh-item1","expected_revision":r,"status":"cancelled","source":"peer"})\n'
                  'print(json.dumps({"status":"bound"} if action=="bind_reminder" else '
                  '{"status":"ok","deliver_allowed":True,"reason":"current","rev":1}))\n') % (
                      str(HERE.parent / 'scripts'), str(u.dir / 'source.sqlite3'))
        helper.write_text(script)
        with self.assertRaisesRegex(ValueError, 'not open|changed'):
            u.claim('hh-item1', self.t0, 'h3', e)
        self.assertIsNone(u.task('hh-item1', self.t0)['in_flight_attempt'])

    def test_existing_timers_and_returns_are_reconciled_not_duplicated(self):
        u = self.u
        for t in ('native', 'legacy', 'legacy-done', 'rule'):
            u.put(t)
        u.plan(self.t0)
        exp = self.t0 + timedelta(days=3, hours=1)
        u.call('cover', self.t0, task_id='native', reminder_id='rem-x', revision=2,
               due_at=tr.iso(self.t0 + timedelta(days=3)), expires_at=tr.iso(exp))
        # legacy mapping verified against a synthetic delivery ledger
        u.ledger = sqlite3.connect(':memory:'); u.ledger.row_factory = sqlite3.Row
        u.ledger.executescript('CREATE TABLE reminders(id TEXT,status TEXT,expires REAL);'
                               'CREATE TABLE events(reminder_id TEXT,status TEXT);')
        u.ledger.execute('INSERT INTO reminders VALUES (?,?,?)', ('L1', 'active', (self.t0 + timedelta(days=2)).timestamp()))
        u.ledger.execute('INSERT INTO reminders VALUES (?,?,?)', ('L2', 'expired', (self.t0 - timedelta(days=2)).timestamp()))
        for i, (rid, ref) in enumerate((('L1', 'task:legacy'), ('L2', 'task:legacy-done'))):
            u.src.execute('INSERT INTO source_schedule_operations VALUES (?,?,?,?,?,?,?,?)',
                          ('op%d' % i, 'd', ref, 1, 'd', '{}', 'mapped', 'r'))
            u.src.execute('INSERT INTO reminder_mappings VALUES (?,?,?,?,?,?,?)', (rid, ref, 1, 'd', 1, 'active', 'op%d' % i))
        u.src.execute('CREATE TABLE note_rules(source_ref TEXT,active INT,remaining INT,invalidated_by TEXT,deadline TEXT)')
        u.src.execute('INSERT INTO note_rules VALUES (?,?,?,?,?)', ('task:rule', 1, 2, None, tr.iso(self.t0 + timedelta(days=1))))
        at = self.t0 + PASS
        p = u.plan(at)
        held = {h['id']: h['reason'] for h in p['held']}
        self.assertEqual(held, {'native': 'covered_by_native_timer', 'legacy': 'covered_by_legacy_timer',
                                'rule': 'covered_by_message_return_rule'})
        self.assertIn('legacy-done', {e['id'] for e in p['eligible']})   # finished timer: not stranded
        later = self.t0 + timedelta(days=4)
        self.assertEqual(set(u.eligible(later)), {'native', 'legacy', 'legacy-done', 'rule'})
        gen = source.meta(u.src, 'generation'); u.plan(later)
        self.assertEqual(source.meta(u.src, 'generation'), gen)          # source never written

    def test_two_private_users_isolated(self):
        a = self.u
        b = User(self.tmp.name, 'beta', zone='America/New_York')
        a.put('shared-id'); b.put('shared-id')
        a.send('shared-id', self.t0, 'i1')
        at = self.t0 + timedelta(hours=8)                    # 18:00 Prague, 12:00 New York
        self.assertTrue(a.eligible(at)['shared-id']['send_blocked'])
        self.assertIsNone(b.eligible(at)['shared-id']['send_blocked'])
        self.assertTrue(b.plan(utc('2027-03-01T04:00:00'))['quiet_now'])  # 23:00 in New York, own zone
        b.review.close()
        with self.assertRaisesRegex(ValueError, 'another source'):
            tr.connect_review(a.dir / 'task-review.sqlite', b.ns)
        self.assertEqual(stat.S_IMODE((a.dir / 'task-review.sqlite').stat().st_mode), 0o600)
        b.review = None; b.close()

    def test_batch_record_is_atomic(self):
        u = self.u; u.put('a'); u.put('b')
        u.plan(self.t0)
        with self.assertRaises(ValueError):
            u.call('record', self.t0, decisions=[{'task_id': 'a', 'attempt_id': 'z1', 'outcome': 'no_message'},
                                                 {'task_id': 'b', 'attempt_id': 'z2', 'outcome': 'sent'}])
        self.assertIsNone(u.task('a', self.t0)['last_review_utc'])


if __name__ == '__main__':
    unittest.main()
