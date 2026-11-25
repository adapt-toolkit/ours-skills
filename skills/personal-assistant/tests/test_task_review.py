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
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'scripts'))
import preference_tools  # noqa: E402
import source_store as source  # noqa: E402
import task_review as tr  # noqa: E402

ZONE = 'Europe/Prague'
QUIET = {'start': '21:00', 'end': '08:00', 'basis': 'user_confirmed', 'confirmed_wire_id': 'synthetic-wire-q'}


def utc(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


class User:
    """One synthetic private user: own dir, source, prefs and sidecar."""

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
        return tr.run(self.review, self.src, {'action': action, 'args': args}, at=at, cfg=self.cfg)

    def plan(self, at):
        return self.call('plan', at)

    def eligible(self, at):
        return {e['id']: e for e in self.plan(at)['eligible']}

    def send(self, tid, at, attempt, outcome='sent'):
        e = self.eligible(at)[tid]
        claim = self.call('begin_send', at, task_id=tid, attempt_id=attempt, source_revision=e['revision'],
                          claim_token=e['claim_token'])
        assert claim['status'] == 'claimed', claim
        args = {'attempt_id': attempt, 'outcome': outcome}
        if outcome == 'sent':
            args['wire_id'] = 'synthetic-wire-' + attempt
        return self.call('send_result', at, **args)


class TaskReviewTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.chmod(self.tmp.name, 0o700)
        self._spread = tr.FIRST_SPREAD_HOURS
        tr.FIRST_SPREAD_HOURS = 1  # deterministic first-review time in most tests
        self.u = User(self.tmp.name, 'alpha')
        self.t0 = utc('2027-03-01T09:00:00')  # 10:00 Prague, outside quiet hours

    def tearDown(self):
        tr.FIRST_SPREAD_HOURS = self._spread
        self.u.src.close(); self.u.review.close(); self.tmp.cleanup()

    def test_silent_open_task_becomes_eligible_again_without_source_change(self):
        u = self.u; u.put('t1')
        self.assertEqual(u.eligible(self.t0), {})  # first-seen grace
        at = self.t0 + timedelta(days=1)
        u.send('t1', at, 'a1')
        self.assertNotIn('t1', u.eligible(at + timedelta(hours=3)))       # no repeat question in 2-3h
        self.assertNotIn('t1', u.eligible(at + timedelta(hours=21)))
        e = u.eligible(at + timedelta(days=1, minutes=1))['t1']           # elapsed time, no hash change
        self.assertIsNone(e['send_allowed_after_utc'])
        self.assertEqual(e['reason'], 'awaiting_response_backoff')

    def test_unchanged_task_no_repeated_questions_and_budget_backoff(self):
        u = self.u; u.put('t1'); u.plan(self.t0)
        at = self.t0 + timedelta(days=1)
        sends = []
        for i in range(12):  # simulate every 2.5h review for many days
            now = at + timedelta(hours=2.5 * i * 4)
            if 't1' in u.eligible(now):
                e = u.eligible(now)['t1']
                if e['send_allowed_after_utc'] is None:
                    u.send('t1', now, 'b%d' % i); sends.append(now)
                else:
                    u.call('record', now, task_id='t1', attempt_id='r%d' % i, outcome='no_message')
        gaps = [b - a for a, b in zip(sends, sends[1:])]
        self.assertTrue(all(g >= timedelta(days=1) for g in gaps), gaps)
        self.assertLessEqual(len(sends), 3)
        row = u.call('status', at, task_id='t1')['task']
        self.assertLessEqual(row['unanswered_sends'], 3)

    def test_source_change_does_not_replenish_send_budget(self):
        u = self.u; u.put('t1'); u.plan(self.t0)
        at = self.t0 + timedelta(days=1)
        u.send('t1', at, 'c1')
        u.put('t1', 'Synthetic task renamed')            # source change
        u.plan(at + timedelta(hours=1))                     # change observed
        later = at + timedelta(hours=4, minutes=1)
        e = u.eligible(later)['t1']                        # review brought forward...
        self.assertIsNotNone(e['send_allowed_after_utc'])  # ...but send cooldown untouched
        with self.assertRaisesRegex(ValueError, 'cooldown'):
            u.call('begin_send', later, task_id='t1', attempt_id='c2', source_revision=e['revision'], claim_token=e['claim_token'])
        u.call('responded', later, task_id='t1', wire_id='synthetic-in-1')  # actual engagement resets
        self.assertEqual(u.call('status', later, task_id='t1')['task']['unanswered_sends'], 0)

    def test_completion_cancellation_and_opt_out_stop_returns(self):
        u = self.u
        for t in ('done', 'gone', 'quiet'):
            u.put(t)
        u.plan(self.t0)
        u.status('done', 'completed'); u.status('gone', 'cancelled')
        at = self.t0 + timedelta(days=2)
        e = u.eligible(at)
        self.assertEqual(set(e), {'quiet'})
        u.call('record', at, task_id='quiet', attempt_id='o1', outcome='opt_out')
        self.assertEqual(u.eligible(at + timedelta(days=90)), {})
        self.assertIsNone(u.plan(at + timedelta(days=90))['schedule'])  # nothing to wake for

    def test_restart_replay_and_uncertain_send(self):
        u = self.u; u.put('t1'); u.plan(self.t0)
        at = self.t0 + timedelta(days=1)
        e = u.eligible(at)['t1']
        claim = u.call('begin_send', at, task_id='t1', attempt_id='u1', source_revision=e['revision'], claim_token=e['claim_token'])
        self.assertTrue(claim['send'])
        # crash before outcome; restart = reconnect
        u.review.close(); u.review = tr.connect_review(u.dir / 'task-review.sqlite', u.ns)
        again = u.call('begin_send', at, task_id='t1', attempt_id='u1', source_revision=e['revision'], claim_token=e['claim_token'])
        self.assertFalse(again['send'])                                  # replay never re-authorizes
        self.assertEqual(u.plan(at + timedelta(days=3))['held'][0]['reason'], 'uncertain_send_reconcile_first')
        u.call('send_result', at, attempt_id='u1', outcome='uncertain')
        with self.assertRaises(ValueError):
            u.call('record', at, task_id='t1', attempt_id='x', outcome='no_message')
        u.call('send_result', at + timedelta(minutes=5), attempt_id='u1', outcome='sent', wire_id='synthetic-found')
        row = u.call('status', at, task_id='t1')['task']
        self.assertEqual((row['unanswered_sends'], row['total_sends'], row['in_flight_attempt']), (1, 1, None))
        self.assertEqual(u.call('send_result', at, attempt_id='u1', outcome='sent', wire_id='synthetic-found')['status'],
                         'already_recorded')
        self.assertEqual(u.call('status', at, task_id='t1')['task']['total_sends'], 1)
        with self.assertRaises(ValueError):  # send outcome without claim
            u.call('send_result', at, attempt_id='never-claimed', outcome='sent', wire_id='w')

    def test_quiet_hours_unconfirmed_hold_and_downtime_catchup_is_bounded(self):
        u = self.u
        for i in range(8):
            u.put('t%d' % i)
        u.plan(self.t0)
        night = utc('2027-03-03T22:30:00')  # 23:30 Prague
        p = u.plan(night)
        self.assertTrue(p['quiet_now']); self.assertEqual(p['eligible'], [])
        self.assertEqual(p['schedule']['args']['local_time'], '2027-03-04T08:00')
        # downtime: wake comes 4 days late; at most 3 tasks surface, rest spaced
        late = utc('2027-03-08T10:00:00')
        p = u.plan(late)
        self.assertEqual(len(p['eligible']), 8)   # one coalesced review pass
        for e in p['eligible'][:3]:
            u.send(e['id'], late, 'd' + e['id'])
        e = u.eligible(late)
        self.assertEqual(len(e), 5)
        first = next(iter(e.values()))
        with self.assertRaisesRegex(ValueError, 'budget'):
            u.call('begin_send', late, task_id=first['id'], attempt_id='d-extra', source_revision=first['revision'],
                   claim_token=first['claim_token'])
        # unconfirmed quiet hours: planning works, sends are held
        v = User(self.tmp.name, 'beta', quiet=None)
        v.put('x'); v.plan(self.t0)
        e = v.eligible(self.t0 + timedelta(days=2))['x']
        self.assertFalse(v.plan(self.t0 + timedelta(days=2))['sends_possible'])
        with self.assertRaisesRegex(ValueError, 'quiet hours not confirmed'):
            v.call('begin_send', self.t0 + timedelta(days=2), task_id='x', attempt_id='q', source_revision=e['revision'],
                   claim_token=e['claim_token'])
        v.src.close(); v.review.close()

    def test_household_tasks_fail_closed_without_role_guard(self):
        u = self.u; u.put('hh-item1'); u.plan(self.t0)
        at = self.t0 + timedelta(days=2)
        e = u.eligible(at)['hh-item1']
        self.assertTrue(e['household_guard_required'])
        with self.assertRaisesRegex(ValueError, 'household'):
            u.call('begin_send', at, task_id='hh-item1', attempt_id='h1', source_revision=e['revision'], claim_token=e['claim_token'])
        # with a guard helper that refuses (stale/conflicted), still no claim
        helper = Path(self.tmp.name) / 'fake_household.py'
        helper.write_text('import sys,json\naction=sys.argv[-1]\n'
                          'print(json.dumps({"status":"bound"} if action=="bind_reminder" else '
                          '{"status":"ok","deliver_allowed":False,"reason":"stale"}))\n')
        u.cfg |= {'household_helper': str(helper), 'household_config': '/nonexistent'}
        with self.assertRaisesRegex(ValueError, 'stale'):
            u.call('begin_send', at, task_id='hh-item1', attempt_id='h2', source_revision=e['revision'], claim_token=e['claim_token'])
        self.assertIsNone(u.call('status', at, task_id='hh-item1')['task']['in_flight_attempt'])

    def test_exact_time_timers_coexist_without_duplicate_channel(self):
        u = self.u; u.put('t1'); u.put('t2'); u.plan(self.t0)
        at = self.t0 + timedelta(days=2)
        due = at + timedelta(days=3)
        u.call('cover', at, task_id='t1', reminder_id='native-synthetic', due_utc=tr.iso(due))
        self.assertNotIn('t1', u.eligible(at))
        self.assertNotIn('t1', u.eligible(due))
        self.assertIn('t1', u.eligible(due + timedelta(hours=1, minutes=1)))
        # legacy mapping: active reminder_mappings row holds the task
        u.src.execute('INSERT INTO source_schedule_operations VALUES (?,?,?,?,?,?,?,?)',
                      ('op1', 'd', 'task:t2', 1, 'd', '{}', 'mapped', 'r'))
        u.src.execute('INSERT INTO reminder_mappings VALUES (?,?,?,?,?,?,?)',
                      ('legacy-1', 'task:t2', 1, 'd', 1, 'active', 'op1'))
        self.assertEqual([h for h in u.plan(at)['held'] if h['id'] == 't2'][0]['reason'], 'covered_by_existing_timer')
        # source untouched by review: generation unchanged
        gen = source.meta(u.src, 'generation'); u.plan(at); self.assertEqual(source.meta(u.src, 'generation'), gen)

    def test_two_private_users_isolated(self):
        a = self.u
        b = User(self.tmp.name, 'beta', zone='America/New_York')
        a.put('shared-id'); b.put('shared-id')
        a.plan(self.t0); b.plan(self.t0)
        at = self.t0 + timedelta(days=2)
        a.send('shared-id', at, 'i1')
        self.assertIn('shared-id', b.eligible(at + timedelta(hours=8)))  # 10:00 New York
        self.assertNotIn('shared-id', a.eligible(at + timedelta(hours=8)))
        b.review.close()
        with self.assertRaisesRegex(ValueError, 'another source'):
            tr.connect_review(a.dir / 'task-review.sqlite', b.ns)
        self.assertEqual(stat.S_IMODE((a.dir / 'task-review.sqlite').stat().st_mode), 0o600)
        b.src.close()

    def test_first_reviews_are_staggered(self):
        tr.FIRST_SPREAD_HOURS = 48
        u = self.u
        for i in range(30):
            u.put('s%d' % i)
        u.plan(self.t0)
        dues = sorted(tr.parse(r[0]) for r in u.review.execute('SELECT next_eligible_utc FROM task_reviews'))
        self.assertGreaterEqual(dues[0], self.t0 + timedelta(hours=24))
        self.assertGreater(dues[-1] - dues[0], timedelta(hours=24))
        # wakes over a week of 'no_message' reviews stay spaced >= 2h and out of quiet hours
        at, wakes = self.t0, []
        for _ in range(200):
            p = u.plan(at)
            for e in p['eligible']:
                u.call('record', at, task_id=e['id'], attempt_id='w%s%s' % (e['id'], tr.iso(at)), outcome='no_message')
            if p['eligible']:
                wakes.append(at)
            s = u.plan(at)['schedule']
            if s is None or s['action'] == 'cancel':
                break
            due = tr.parse(s['due_utc'])
            u.call('chain', at, reminder_id='c', revision=1, request_key=s['args'].get('request_key', 'k'), due_at=s['due_utc'])
            at = due
            if at > self.t0 + timedelta(days=7):
                break
        gaps = [b - a for a, b in zip(wakes, wakes[1:])]
        self.assertTrue(all(g >= timedelta(hours=2) for g in gaps), gaps)
        self.assertLessEqual(len(wakes), 7 * 7)
        from zoneinfo import ZoneInfo
        self.assertFalse(any(tr.in_quiet(w, ZoneInfo(ZONE), QUIET) for w in wakes))

    def test_single_chain_and_reschedule(self):
        u = self.u; u.put('t1')
        p = u.plan(self.t0)
        s = p['schedule']
        self.assertEqual(s['action'], 'create')
        self.assertEqual(s['args']['late_window_seconds'], 604800)
        u.call('chain', self.t0, reminder_id='rem-1', revision=1, request_key=s['args']['request_key'],
               due_at=s['due_utc'])
        self.assertIsNone(u.plan(self.t0 + timedelta(minutes=1))['schedule'])  # one chain, no duplicate
        u.put('t2')
        # after the chain wake, a successor is requested
        after = tr.parse(s['due_utc']) + timedelta(minutes=1)
        self.assertEqual(u.plan(after)['schedule']['action'], 'create')
        # no open tasks: nothing scheduled, current chain cancelled
        u.status('t1', 'completed'); u.status('t2', 'completed')
        u.call('chain', after, reminder_id='rem-2', revision=1, request_key='k2', due_at=tr.iso(after + timedelta(hours=5)))
        self.assertEqual(u.plan(after)['schedule']['action'], 'cancel')


if __name__ == '__main__':
    unittest.main()
