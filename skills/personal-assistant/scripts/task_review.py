#!/usr/bin/env python3
"""Durable periodic reconsideration of open source tasks.

Each periodic review pass (an explicitly configured Fleet role loop) runs
`plan`: a deterministic scan of the configured schema2 source, opened
read-only, that lists which open tasks are due for reconsideration. Review and
send accounting live in a separate private sidecar database. The source store,
its writer, preferences, timers, returns and delivery ledgers are never written
here. This script sends nothing; an outward message needs a `begin_send` claim.
"""
import argparse
from datetime import datetime, time as dtime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from zoneinfo import ZoneInfo

import preference_tools
import source_store as source
from legacy_source import pairs, read_private_json

SCHEMA_VERSION = '1'
SCHEMA = '''
CREATE TABLE review_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE task_reviews(
 task_id TEXT PRIMARY KEY,
 seen_revision INTEGER NOT NULL,seen_ready INTEGER NOT NULL,
 first_seen_utc TEXT NOT NULL,last_review_utc TEXT,last_outcome TEXT,
 last_send_utc TEXT,last_send_wire TEXT,last_send_outcome TEXT,
 unanswered_sends INTEGER NOT NULL DEFAULT 0 CHECK(unanswered_sends>=0),
 total_sends INTEGER NOT NULL DEFAULT 0,send_state_key TEXT,
 next_eligible_utc TEXT NOT NULL,reason TEXT NOT NULL,
 opted_out INTEGER NOT NULL DEFAULT 0 CHECK(opted_out IN (0,1)),
 covered_by TEXT,covered_until_utc TEXT,
 in_flight_attempt TEXT,closed_status TEXT);
CREATE TABLE review_log(
 attempt_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,at_utc TEXT NOT NULL,
 outcome TEXT NOT NULL,source_revision INTEGER NOT NULL,note TEXT);
CREATE TABLE send_claims(
 attempt_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,claimed_utc TEXT NOT NULL,
 source_revision INTEGER NOT NULL,guard_json TEXT,
 state TEXT NOT NULL CHECK(state IN ('claimed','sent','uncertain','failed','not_sent')),
 wire_id TEXT,closed_utc TEXT);
CREATE INDEX send_claims_time ON send_claims(claimed_utc);
CREATE TABLE responses(wire_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,incoming_utc TEXT NOT NULL);
'''

# Policy defaults (assistant-selected; documented in references/task-review.md).
REVIEW_INTERVAL = timedelta(hours=2)      # unchanged task: reconsidered on (about) every pass
CHANGE_GRACE = timedelta(minutes=30)      # source change: let the live conversation settle
SEND_BACKOFF = [timedelta(days=1), timedelta(days=2), timedelta(days=4)]
MAX_UNANSWERED = 3                        # then no send until a meaningful change or a reply
MAX_DEFER = timedelta(days=60)
SEND_WINDOW = timedelta(hours=2)
MAX_SENDS_PER_WINDOW = 3                  # outward review messages across tasks per window
MAX_REVIEW_PER_PASS = 20                  # least recently reviewed first; rest next pass
MAX_BATCH = 50
REVIEW_OUTCOMES = ('no_message', 'deferred', 'opt_out', 'opt_in')
SEND_OUTCOMES = ('sent', 'uncertain', 'failed', 'not_sent')


def now_utc():
    return datetime.now(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def parse(value):
    if not isinstance(value, str) or not value.endswith('Z'):
        raise ValueError('UTC timestamp ending Z required')
    return datetime.fromisoformat(value[:-1] + '+00:00')


def when(value):
    """Ledger/extension timestamps: epoch seconds or ISO with offset/Z."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc)
    v = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return v if v.utcoffset() is not None else v.replace(tzinfo=timezone.utc)


def text(value, limit=200):
    if not isinstance(value, str) or not value.strip() or len(value) > limit or '\x00' in value:
        raise ValueError('bounded text required')
    return value


def connect_review(path, namespace, initialize=False):
    """Sidecar is private (0600 in a 0700 dir), bound to exactly one source namespace."""
    path = Path(path)
    if initialize and not path.exists():
        source.private_path(path, create=True)
    source.private_path(path)
    db = sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, isolation_level=None, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA synchronous=FULL')
    try:
        if db.execute('SELECT 1 FROM sqlite_master LIMIT 1').fetchone() is None:
            if not initialize:
                raise ValueError('task review store not initialized')
            db.execute('BEGIN IMMEDIATE')
            for statement in SCHEMA.split(';'):
                if statement.strip():
                    db.execute(statement)
            db.executemany('INSERT INTO review_meta VALUES (?,?)',
                           [('schema_version', SCHEMA_VERSION), ('source_namespace', namespace)])
            db.commit()
        meta = dict(db.execute('SELECT key,value FROM review_meta').fetchall())
        if meta.get('schema_version') != SCHEMA_VERSION or meta.get('source_namespace') != namespace:
            raise ValueError('task review store bound to another source or schema')
        return db
    except BaseException:
        if db.in_transaction:
            db.rollback()
        db.close()
        raise


def _hm(value):
    if not isinstance(value, str) or len(value) != 5:
        raise ValueError('HH:MM required')
    return dtime.fromisoformat(value)


def quiet_window(review, cfg=None):
    """User-confirmed quiet hours (sidecar, else operator-recorded confirmation), or None."""
    row = review.execute("SELECT value FROM review_meta WHERE key='quiet_hours'").fetchone()
    if row is not None:
        return json.loads(row[0])
    bound = (cfg or {}).get('task_review_quiet_hours')
    if (isinstance(bound, dict) and bound.get('basis') == 'user_confirmed'
            and isinstance(bound.get('confirmed_wire_id'), str) and bound['confirmed_wire_id']):
        if bound.get('none') is True:  # the user explicitly confirmed having NO quiet hours
            if set(bound) - {'none', 'basis', 'confirmed_wire_id', 'confirmed_at_utc'}:
                raise ValueError('confirmed no-quiet-hours takes no window')
            return {k: bound[k] for k in ('none', 'basis', 'confirmed_wire_id')}
        if _hm(bound['start']) == _hm(bound['end']):
            raise ValueError('invalid configured quiet hours')
        return {k: bound[k] for k in ('start', 'end', 'basis', 'confirmed_wire_id')}
    return None


def in_quiet(at, zone, quiet):
    if quiet.get('none') is True:
        return False
    local = at.astimezone(zone).time()
    start, end = _hm(quiet['start']), _hm(quiet['end'])
    return (start <= local < end) if start < end else (local >= start or local < end)


def confirmed_zone(src):
    zone = preference_tools.read(src, ['timezone'])
    if zone.get('confirmed') is not True:
        raise ValueError('confirmed user timezone required before periodic review')
    return ZoneInfo(zone['timezone'])


def open_tasks(src):
    rows = src.execute('''SELECT t.id,t.title,t.status,t.revision,t.archived,
      NOT EXISTS(SELECT 1 FROM prerequisites p JOIN tasks q ON q.id=p.prerequisite_id
                 WHERE p.task_id=t.id AND q.status!='completed') AS ready
      FROM tasks t''').fetchall()
    return {r['id']: r for r in rows}


def _has_table(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _rows(db, table, sql, args):
    """An absent optional extension table means 'no such mechanism'. Any other failure
    propagates (fail closed) instead of being read as absence."""
    if not _has_table(db, table):
        return []
    return db.execute(sql, args).fetchall()


# Real legacy ledger semantics: event admission `status` and transport `delivery_status` are
# separate; follow-up children extend the lineage of the mapped reminder.
LEDGER_LIVE = """WITH RECURSIVE lineage(id) AS (
  SELECT ? UNION SELECT f.child_id FROM lineage l JOIN events e ON e.reminder_id=l.id
  JOIN followups f ON f.parent_event_id=e.event_id)
SELECT
 (SELECT MAX(r.expires) FROM reminders r JOIN lineage l ON r.id=l.id WHERE r.status='active') AS active_until,
 EXISTS(SELECT 1 FROM events e JOIN lineage l ON e.reminder_id=l.id
        WHERE e.delivery_status IN ('sending','uncertain')) AS transport_uncertain,
 EXISTS(SELECT 1 FROM events e JOIN lineage l ON e.reminder_id=l.id JOIN reminders r ON r.id=e.reminder_id
        WHERE e.delivery_status IS NULL AND e.status IN ('pending','submitting','queued','uncertain')
          AND r.status='active' AND r.revision=e.revision AND r.expires>?) AS admitted_live"""


def existing_mechanism(src, ledger, task_id, at):
    """(kind, until) when an existing timer/return already owns this task's next return.
    Legacy mappings are verified against the delivery ledger, so a naturally finished reminder
    does not suppress the task forever, while any queued/sending/uncertain delivery still holds it."""
    ref = 'task:' + task_id
    if _rows(src, 'source_schedule_operations',
             "SELECT 1 FROM source_schedule_operations WHERE source_ref=? AND status='prepared' LIMIT 1", (ref,)):
        return 'pending_schedule_operation', None
    for m in _rows(src, 'reminder_mappings',
                   "SELECT reminder_id FROM reminder_mappings WHERE source_ref=? AND status='active'", (ref,)):
        if ledger is None:
            return 'legacy_timer_unverified', None
        if not all(_has_table(ledger, t) for t in ('reminders', 'events', 'followups')):
            raise ValueError('configured delivery ledger has an unrecognized schema')
        root = ledger.execute('SELECT source_ref FROM reminders WHERE id=?', (m['reminder_id'],)).fetchone()
        if root is None or (root['source_ref'] is not None and root['source_ref'] != ref):
            return 'legacy_timer_unverified', None  # missing/mismatched root: never read as "no timer"
        live = ledger.execute(LEDGER_LIVE, (m['reminder_id'], at.timestamp())).fetchone()
        if live['transport_uncertain'] or live['admitted_live']:
            return 'legacy_timer_in_delivery', None
        if live['active_until'] is not None and when(live['active_until']) > at:
            return 'legacy_timer', when(live['active_until'])
    for r in _rows(src, 'note_rules', 'SELECT deadline FROM note_rules WHERE source_ref=? AND active=1 '
                   'AND remaining>0 AND invalidated_by IS NULL', (ref,)):
        if when(r['deadline']) > at:
            return 'message_return_rule', when(r['deadline'])
    for r in _rows(src, 'return_decisions', "SELECT deadline FROM return_decisions WHERE source_ref=? "
                   "AND decision IN ('planned_timed','planned_message','committed_message')", (ref,)):
        if r['deadline'] is None:
            return 'contextual_return', None
        if when(r['deadline']) > at:
            return 'contextual_return', when(r['deadline'])
    if _rows(src, 'task_trigger_reservations',
             "SELECT 1 FROM task_trigger_reservations WHERE task_id=? AND status='prepared'", (task_id,)):
        return 'task_trigger_pending', None
    return None, None


def state_key(t):
    return '%d:%d' % (t['revision'], int(t['ready']))


def send_cooldown_until(row):
    if row['last_send_utc'] is None or row['unanswered_sends'] == 0:
        return None
    n = row['unanswered_sends']
    return parse(row['last_send_utc']) + SEND_BACKOFF[min(n - 1, len(SEND_BACKOFF) - 1)]


def send_block(row, t, at):
    """Why an outward review message is not allowed now (None if allowed). The cooldown runs
    from the actual last send; an exhausted unanswered budget stays exhausted until the task
    meaningfully changes after that send or the user responds."""
    if row['unanswered_sends'] >= MAX_UNANSWERED and row['send_state_key'] == state_key(t):
        return 'repeat_budget_exhausted_unchanged'
    cooldown = send_cooldown_until(row)
    if cooldown is not None and cooldown > at:
        return 'send_cooldown_until_' + iso(cooldown)
    return None


def sync(review, src, at):
    """Observe source; never writes source. Returns current task rows."""
    tasks = open_tasks(src)
    for task_id, t in tasks.items():
        row = review.execute('SELECT * FROM task_reviews WHERE task_id=?', (task_id,)).fetchone()
        live = t['status'] == 'open' and not t['archived']
        if row is None:
            if live:
                review.execute('''INSERT INTO task_reviews(task_id,seen_revision,seen_ready,first_seen_utc,
                    next_eligible_utc,reason) VALUES (?,?,?,?,?,?)''',
                               (task_id, t['revision'], int(t['ready']), iso(at), iso(at), 'first_seen'))
            continue
        if not live:
            if row['closed_status'] is None:
                review.execute('UPDATE task_reviews SET closed_status=?,reason=? WHERE task_id=?',
                               ('archived' if t['archived'] else t['status'], 'source_closed', task_id))
            continue
        if row['closed_status'] is not None:  # explicitly reopened in source
            review.execute('UPDATE task_reviews SET closed_status=NULL WHERE task_id=?', (task_id,))
        if t['revision'] != row['seen_revision'] or int(t['ready']) != row['seen_ready']:
            # A meaningful change brings REVIEW forward; it never touches send cooldown/budget.
            target = min(parse(row['next_eligible_utc']), at + CHANGE_GRACE)
            why = 'became_ready' if t['ready'] and not row['seen_ready'] else 'source_changed'
            review.execute('UPDATE task_reviews SET seen_revision=?,seen_ready=?,next_eligible_utc=?,reason=? WHERE task_id=?',
                           (t['revision'], int(t['ready']), iso(target), why, task_id))
    return tasks


def claim_token(row, t):
    state = [row['task_id'], t['revision'], int(t['ready']), row['next_eligible_utc'], row['last_send_utc'],
             row['unanswered_sends'], row['send_state_key'], row['opted_out'], row['in_flight_attempt'],
             row['covered_until_utc']]
    return hashlib.sha256(json.dumps(state).encode()).hexdigest()[:24]


def classify(src, ledger, row, t, at):
    """Deterministic review eligibility of one tracked task now: (state, detail)."""
    if t is None:
        return 'held', 'source_missing'
    if row['opted_out']:
        return 'opted_out', 'user_opted_out'
    if row['in_flight_attempt']:
        return 'held', 'uncertain_send_reconcile_first'
    kind, _ = existing_mechanism(src, ledger, row['task_id'], at)
    if kind is not None:
        return 'held', 'covered_by_' + kind  # reconcile, never duplicate its purpose
    if row['covered_until_utc'] and parse(row['covered_until_utc']) > at:
        return 'held', 'covered_by_native_timer'
    if parse(row['next_eligible_utc']) > at:
        return 'waiting', row['reason']
    return 'eligible', row['reason']


def plan(review, src, at, cfg=None, ledger=None):
    zone = confirmed_zone(src)
    quiet = quiet_window(review, cfg)
    quiet_now = quiet is not None and in_quiet(at, zone, quiet)
    with source.atomic(review, True):
        with source.atomic(src):
            tasks = sync(review, src, at)
            rows = review.execute('''SELECT * FROM task_reviews WHERE closed_status IS NULL
                ORDER BY COALESCE(last_review_utc,''),task_id''').fetchall()
            states = [(r, tasks.get(r['task_id'])) + classify(src, ledger, r, tasks.get(r['task_id']), at) for r in rows]
    eligible, held, waiting = [], [], 0
    for r, t, state, detail in states:
        if state == 'eligible':
            eligible.append({'id': r['task_id'], 'title': t['title'], 'revision': t['revision'],
                             'ready': bool(t['ready']), 'reason': detail,
                             'last_review_utc': r['last_review_utc'], 'last_outcome': r['last_outcome'],
                             'last_send_utc': r['last_send_utc'], 'unanswered_sends': r['unanswered_sends'],
                             'send_blocked': send_block(r, t, at),
                             'household_guard_required': r['task_id'].startswith('hh-'),
                             'claim_token': claim_token(r, t)})
        elif state == 'held':
            held.append({'id': r['task_id'], 'reason': detail})
        elif state == 'waiting':
            waiting += 1
    batch = [] if quiet_now else eligible[:MAX_REVIEW_PER_PASS]
    return {'status': 'ok', 'data_only': True, 'now_utc': iso(at), 'time_zone': zone.key,
            'quiet_hours': quiet, 'quiet_now': quiet_now,
            'sends_possible': quiet is not None and not quiet_now,
            'eligible': batch, 'eligible_next_pass': len(eligible) - len(batch),
            'held': held, 'waiting': waiting, 'open_tracked': len(rows),
            'action': 'end_turn_quietly' if not batch else 'review_listed_tasks'}


def _task_row(review, task_id):
    row = review.execute('SELECT * FROM task_reviews WHERE task_id=?', (source.identifier(task_id),)).fetchone()
    if row is None:
        raise ValueError('task not tracked; run plan first')
    return row


def _decide(review, src, data, at):
    if not isinstance(data, dict) or set(data) - {'task_id', 'attempt_id', 'outcome', 'note', 'next_review_utc'} or \
            not {'task_id', 'attempt_id', 'outcome'} <= set(data):
        raise ValueError('task_id, attempt_id and outcome required')
    outcome = data['outcome']
    if outcome not in REVIEW_OUTCOMES:
        raise ValueError('review outcome must be no_message|deferred|opt_out|opt_in; sends use begin_send/send_result')
    attempt = text(data['attempt_id'], 128)
    note = None if data.get('note') is None else text(data['note'], 300)
    row = _task_row(review, data['task_id'])
    prior = review.execute('SELECT * FROM review_log WHERE attempt_id=?', (attempt,)).fetchone()
    if prior is not None:
        if prior['outcome'] == outcome and prior['task_id'] == row['task_id']:
            return {'status': 'already_recorded', 'attempt_id': attempt, 'task_id': row['task_id']}
        raise ValueError('attempt_id already used')
    t = open_tasks(src).get(row['task_id'])
    rev = t['revision'] if t else row['seen_revision']
    if outcome in ('no_message', 'deferred') and row['in_flight_attempt']:
        raise ValueError('reconcile the in-flight send claim first')
    wait = {'no_message': REVIEW_INTERVAL, 'deferred': REVIEW_INTERVAL, 'opt_out': MAX_DEFER, 'opt_in': timedelta(0)}[outcome]
    reason = {'opt_out': 'user_opted_out', 'opt_in': 'user_opted_in'}.get(outcome, outcome)
    nxt = at + wait
    if data.get('next_review_utc') is not None and outcome in ('no_message', 'deferred'):
        chosen = parse(data['next_review_utc'])
        if not at + timedelta(minutes=30) <= chosen <= at + MAX_DEFER:
            raise ValueError('next_review_utc must be 30 minutes to 60 days ahead')
        nxt, reason = chosen, reason + ':assistant_selected_time'
    if outcome in ('opt_out', 'opt_in'):
        review.execute('UPDATE task_reviews SET opted_out=? WHERE task_id=?', (int(outcome == 'opt_out'), row['task_id']))
    review.execute('UPDATE task_reviews SET last_review_utc=?,last_outcome=?,next_eligible_utc=?,reason=? WHERE task_id=?',
                   (iso(at), outcome, iso(nxt), reason, row['task_id']))
    review.execute('INSERT INTO review_log VALUES (?,?,?,?,?,?)', (attempt, row['task_id'], iso(at), outcome, rev, note))
    return {'status': 'recorded', 'attempt_id': attempt, 'task_id': row['task_id'], 'outcome': outcome,
            'next_eligible_utc': iso(nxt), 'reason': reason}


def record(review, src, data, at, cfg=None, ledger=None):
    """Truthful non-send review decision(s). `decisions` records several in one transaction."""
    with source.atomic(review, True):
        if 'decisions' in data:
            if set(data) != {'decisions'} or not isinstance(data['decisions'], list) or not 1 <= len(data['decisions']) <= MAX_BATCH:
                raise ValueError('decisions: 1..50 decision objects')
            return {'status': 'recorded', 'results': [_decide(review, src, d, at) for d in data['decisions']]}
        return _decide(review, src, data, at)


def household_guard(cfg, task_id, attempt):
    """Fail closed: hh- sends must pass the role's own household helper, never a model flag."""
    helper, config = (cfg or {}).get('household_helper'), (cfg or {}).get('household_config')
    if not helper or not config:
        raise ValueError('household-linked task without configured household guard; send refused')
    ref, rid = 'task:' + task_id, 'task-review:' + attempt

    def call(action, payload):
        out = subprocess.run([sys.executable, '-I', helper, '--config', config, action], input=json.dumps(payload),
                             capture_output=True, text=True, timeout=20)
        try:
            return json.loads(out.stdout)
        except ValueError:
            return {'status': 'error'}
    if call('bind_reminder', {'source_ref': ref, 'reminder_id': rid}).get('status') != 'bound':
        raise ValueError('household item not current; send refused')
    guard = call('due_guard', {'source_ref': ref, 'reminder_id': rid})
    if guard.get('deliver_allowed') is not True:
        raise ValueError('household due_guard refused: ' + str(guard.get('reason'))[:60])
    return {'reason': guard.get('reason'), 'rev': guard.get('rev'), 'binding': rid}


def _claimable(review, src, ledger, row, data, at):
    with source.atomic(src):
        t = open_tasks(src).get(row['task_id'])
        if t is None or t['status'] != 'open' or t['archived'] or row['closed_status']:
            raise ValueError('task is not open in source')
        if row['in_flight_attempt']:
            raise ValueError('another send claim is in flight; reconcile it first')
        if t['revision'] != data['source_revision'] or claim_token(row, t) != data['claim_token']:
            raise ValueError('stale claim; run plan again')
        state, detail = classify(src, ledger, row, t, at)
    if state != 'eligible':
        raise ValueError('task not eligible: ' + detail)
    blocked = send_block(row, t, at)
    if blocked:
        raise ValueError('send not allowed: ' + blocked)
    return t


def begin_send(review, src, data, at, cfg=None, ledger=None):
    """Durable CAS claim BEFORE any outward review message. Every gate is evaluated inside one
    sidecar write transaction against fresh source snapshots taken before AND after the
    household guard, which runs while the sidecar lock is held."""
    if set(data) != {'task_id', 'attempt_id', 'source_revision', 'claim_token'}:
        raise ValueError('task_id, attempt_id, source_revision and claim_token from plan required')
    attempt = text(data['attempt_id'], 128)
    with source.atomic(review, True):
        prior = review.execute('SELECT * FROM send_claims WHERE attempt_id=?', (attempt,)).fetchone()
        if prior is not None:
            # Replay never re-authorizes: an existing claim is closed via send_result only.
            return {'status': 'already_claimed', 'attempt_id': attempt, 'state': prior['state'], 'send': False}
        quiet = quiet_window(review, cfg)
        if quiet is None:
            raise ValueError('quiet hours not confirmed by the user; review messages held')
        if in_quiet(at, confirmed_zone(src), quiet):
            raise ValueError('inside confirmed quiet hours; send held')
        if review.execute('SELECT COUNT(*) FROM send_claims WHERE claimed_utc>?',
                          (iso(at - SEND_WINDOW),)).fetchone()[0] >= MAX_SENDS_PER_WINDOW:
            raise ValueError('review send budget for this window used')
        row = _task_row(review, data['task_id'])
        t = _claimable(review, src, ledger, row, data, at)
        guard = household_guard(cfg, row['task_id'], attempt) if row['task_id'].startswith('hh-') else None
        if state_key(_claimable(review, src, ledger, row, data, at)) != state_key(t):
            raise ValueError('source changed during claim; run plan again')
        review.execute('UPDATE task_reviews SET in_flight_attempt=?,send_state_key=? WHERE task_id=?',
                       (attempt, state_key(t), row['task_id']))
        review.execute('INSERT INTO send_claims(attempt_id,task_id,claimed_utc,source_revision,guard_json,state) VALUES (?,?,?,?,?,?)',
                       (attempt, row['task_id'], iso(at), t['revision'], None if guard is None else json.dumps(guard), 'claimed'))
    return {'status': 'claimed', 'attempt_id': attempt, 'task_id': row['task_id'], 'send': True, 'household_guard': guard}


def send_result(review, data, at, cfg=None):
    """Record the ACTUAL transport outcome of a claimed attempt. `uncertain` keeps it held."""
    if set(data) - {'attempt_id', 'outcome', 'wire_id', 'evidence'} or not {'attempt_id', 'outcome'} <= set(data):
        raise ValueError('attempt_id and outcome required')
    outcome = data['outcome']
    if outcome not in SEND_OUTCOMES:
        raise ValueError('outcome must be sent|uncertain|failed|not_sent')
    with source.atomic(review, True):
        claim = review.execute('SELECT * FROM send_claims WHERE attempt_id=?', (text(data['attempt_id'], 128),)).fetchone()
        if claim is None:
            raise ValueError('no claim for this attempt_id; begin_send must precede any send')
        if claim['state'] not in ('claimed', 'uncertain'):
            if claim['state'] == outcome:
                return {'status': 'already_recorded', 'attempt_id': claim['attempt_id'], 'state': claim['state']}
            raise ValueError('claim already closed as ' + claim['state'])
        if claim['state'] == 'uncertain' and outcome == 'uncertain':
            return {'status': 'already_recorded', 'attempt_id': claim['attempt_id'], 'state': 'uncertain'}
        row = _task_row(review, claim['task_id'])
        if outcome == 'uncertain':
            review.execute("UPDATE send_claims SET state='uncertain' WHERE attempt_id=?", (claim['attempt_id'],))
            review.execute("UPDATE task_reviews SET last_send_outcome='uncertain',reason='uncertain_send_reconcile_first' WHERE task_id=?",
                           (row['task_id'],))
            return {'status': 'held_uncertain', 'attempt_id': claim['attempt_id'],
                    'action': 'check your own outbound history, then send_result sent(+wire_id) or not_sent'}
        if claim['state'] == 'uncertain' and outcome in ('failed', 'not_sent'):
            # Absence from history is not proof of non-delivery: require a definite transport result.
            if data.get('evidence') not in ('transport_rejected', 'never_submitted'):
                raise ValueError('uncertain send stays held without definite evidence (transport_rejected|never_submitted)')
        wire = text(data.get('wire_id'), 256) if outcome == 'sent' else None
        review.execute('UPDATE send_claims SET state=?,wire_id=?,closed_utc=? WHERE attempt_id=?',
                       (outcome, wire, iso(at), claim['attempt_id']))
        if outcome == 'sent':
            sent_at = at  # transport confirmation time: a conservative bound, never before the actual send
            n = row['unanswered_sends'] + 1
            reason = 'awaiting_response' if n < MAX_UNANSWERED else 'repeat_budget_exhausted'
            review.execute('''UPDATE task_reviews SET last_review_utc=?,last_outcome='sent',last_send_utc=?,last_send_wire=?,
                last_send_outcome='sent',unanswered_sends=?,total_sends=total_sends+1,in_flight_attempt=NULL,
                next_eligible_utc=?,reason=? WHERE task_id=?''',
                           (iso(at), iso(sent_at), wire, n, iso(at + REVIEW_INTERVAL), reason, row['task_id']))
        else:
            reason = 'send_' + outcome
            review.execute('''UPDATE task_reviews SET last_review_utc=?,last_outcome=?,last_send_outcome=?,in_flight_attempt=NULL,
                next_eligible_utc=?,reason=? WHERE task_id=?''', (iso(at), outcome, outcome, iso(at + REVIEW_INTERVAL), reason, row['task_id']))
    return {'status': 'recorded', 'attempt_id': claim['attempt_id'], 'task_id': row['task_id'], 'outcome': outcome,
            'reason': reason}


def responded(review, data, at, cfg=None):
    """The user actually engaged with this task: clears the unanswered count, once per wire, and
    only for a message that arrived after the last review message."""
    if set(data) != {'task_id', 'wire_id', 'incoming_utc'}:
        raise ValueError('task_id, actual incoming wire_id and its incoming_utc required')
    wire = text(data['wire_id'], 256)
    incoming = parse(data['incoming_utc'])
    if incoming > at + timedelta(minutes=5):
        raise ValueError('incoming_utc in the future')
    with source.atomic(review, True):
        row = _task_row(review, data['task_id'])
        prior = review.execute('SELECT task_id FROM responses WHERE wire_id=?', (wire,)).fetchone()
        if prior is not None:
            return {'status': 'already_recorded', 'task_id': prior['task_id']}
        review.execute('INSERT INTO responses VALUES (?,?,?)', (wire, row['task_id'], iso(incoming)))
        if row['last_send_utc'] is not None and incoming <= parse(row['last_send_utc']):
            return {'status': 'recorded_no_reset', 'task_id': row['task_id'], 'reason': 'response_predates_last_send'}
        review.execute("UPDATE task_reviews SET unanswered_sends=0,reason='user_responded' WHERE task_id=?", (row['task_id'],))
    return {'status': 'recorded', 'task_id': row['task_id']}


def _cover_ref(data):
    if type(data.get('revision')) is not int or data['revision'] < 1:
        raise ValueError('positive reminder revision required')
    return text(data.get('reminder_id'), 128), data['revision']


def cover(review, data, at, cfg=None):
    """Register / update / clear the ACTUAL native self-reminder receipt that owns this task's next
    return. Compare-and-swap on the stored receipt: a stale or different receipt can never erase or
    replace a live hold. Coverage lasts until that reminder's late-window expiry."""
    with source.atomic(review, True):
        row = _task_row(review, data.get('task_id'))
        live = row['covered_by'] if row['covered_until_utc'] and parse(row['covered_until_utc']) > at else None
        cur = None if live is None else (live.rsplit('@', 1)[0], int(live.rsplit('@', 1)[1]))
        if data.get('clear') is True:
            if set(data) != {'task_id', 'clear', 'reminder_id', 'revision'}:
                raise ValueError('task_id, clear:true and the covering reminder_id/revision required')
            rid, rev = _cover_ref(data)
            if cur is None:
                return {'status': 'nothing_to_clear', 'task_id': row['task_id']}
            if cur[0] != rid or rev < cur[1]:
                raise ValueError('stale or different reminder receipt; live hold kept')
            review.execute('UPDATE task_reviews SET covered_by=NULL,covered_until_utc=NULL WHERE task_id=?', (row['task_id'],))
            return {'status': 'cleared', 'task_id': row['task_id']}
        if set(data) - {'task_id', 'reminder_id', 'revision', 'due_at', 'expires_at', 'replaces'} or \
                not {'task_id', 'reminder_id', 'revision', 'due_at', 'expires_at'} <= set(data):
            raise ValueError('task_id plus the actual reminder id, revision, due_at and expires_at required')
        rid, rev = _cover_ref(data)
        due, expires = when(text(data['due_at'], 64)), when(text(data['expires_at'], 64))
        if not due <= expires or expires <= at or expires > at + timedelta(days=1840):
            raise ValueError('current reminder receipt required')
        if cur is not None:
            if cur[0] == rid and rev < cur[1]:
                raise ValueError('stale reminder revision; live hold kept')
            if cur[0] != rid and data.get('replaces') != '%s@%d' % cur:
                raise ValueError('another reminder holds this task; pass replaces=<id@revision> after reconciling it')
        review.execute('UPDATE task_reviews SET covered_by=?,covered_until_utc=? WHERE task_id=?',
                       ('%s@%d' % (rid, rev), iso(expires), row['task_id']))
    return {'status': 'covered', 'task_id': row['task_id'], 'until_utc': iso(expires)}


def set_quiet_hours(review, data, at, cfg=None):
    if set(data) == {'none', 'confirmed_wire_id'} and data['none'] is True:
        value = {'none': True, 'basis': 'user_confirmed',
                 'confirmed_wire_id': text(data['confirmed_wire_id'], 256), 'confirmed_at_utc': iso(at)}
    else:
        if set(data) != {'start', 'end', 'confirmed_wire_id'}:
            raise ValueError('start, end (HH:MM) and the user confirmation wire, or none:true, required')
        if _hm(data['start']) == _hm(data['end']):
            raise ValueError('empty or full-day quiet window unsupported; use none:true for a confirmed empty list')
        value = {'start': data['start'], 'end': data['end'], 'basis': 'user_confirmed',
                 'confirmed_wire_id': text(data['confirmed_wire_id'], 256), 'confirmed_at_utc': iso(at)}
    with source.atomic(review, True):
        review.execute("INSERT OR REPLACE INTO review_meta VALUES ('quiet_hours',?)", (json.dumps(value),))
    return {'status': 'quiet_hours_set', 'quiet_hours': value}


def status(review, data, at, cfg=None):
    """Operator/agent summary without private titles."""
    if set(data) - {'task_id'}:
        raise ValueError('optional task_id only')
    if 'task_id' in data:
        row = _task_row(review, data['task_id'])
        return {'status': 'ok', 'task': dict(row),
                'claims': [dict(r) for r in review.execute(
                    'SELECT attempt_id,claimed_utc,state,wire_id FROM send_claims WHERE task_id=? ORDER BY claimed_utc DESC LIMIT 5',
                    (row['task_id'],))]}
    counts = dict(review.execute('''SELECT CASE WHEN closed_status IS NOT NULL THEN 'closed' WHEN opted_out THEN 'opted_out'
        WHEN in_flight_attempt IS NOT NULL THEN 'in_flight' ELSE 'tracked' END,COUNT(*) FROM task_reviews GROUP BY 1''').fetchall())
    last = review.execute('SELECT MAX(at_utc) FROM review_log').fetchone()[0]
    return {'status': 'ok', 'counts': counts, 'quiet_hours': quiet_window(review, cfg), 'last_review_decision_utc': last}


WITH_SOURCE = {'begin_send': begin_send, 'record': record}
REVIEW_ONLY = {'send_result': send_result, 'responded': responded, 'cover': cover,
               'set_quiet_hours': set_quiet_hours, 'status': status}


def run(review, src, request, at=None, cfg=None, ledger=None):
    at = at or now_utc()
    if not isinstance(request, dict) or set(request) - {'action', 'args'} or 'action' not in request:
        raise ValueError('{"action":...,"args":{...}} required')
    action, args = request['action'], request.get('args', {})
    if not isinstance(args, dict):
        raise ValueError('args object required')
    if action == 'plan':
        if args:
            raise ValueError('plan takes no arguments')
        return plan(review, src, at, cfg, ledger)
    if action in WITH_SOURCE:
        return WITH_SOURCE[action](review, src, args, at, cfg, ledger)
    if action in REVIEW_ONLY:
        return REVIEW_ONLY[action](review, args, at, cfg)
    raise ValueError('unsupported action')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', required=True)
    p.add_argument('--init', action='store_true', help='operator: create the empty sidecar once')
    opts = p.parse_args()
    src = review = ledger = None
    try:
        cfg = read_private_json(Path(opts.config))
        if cfg.get('source_schema') != 2 or not cfg.get('source_db') or not cfg.get('task_review_db'):
            print(json.dumps({'status': 'not_configured', 'data_only': True}))
            return 1
        src = source.connect(Path(cfg['source_db']), readonly=True)
        namespace = source.meta(src, 'namespace')
        if namespace != cfg.get('source_namespace'):
            raise ValueError('source namespace binding mismatch')
        review = connect_review(cfg['task_review_db'], namespace, initialize=opts.init)
        if opts.init:
            print(json.dumps({'status': 'initialized', 'data_only': True}))
            return 0
        if cfg.get('database'):  # legacy delivery ledger, read-only, to verify mapped timers
            ledger = sqlite3.connect(Path(cfg['database']).as_uri() + '?mode=ro', uri=True, timeout=5)
            ledger.row_factory = sqlite3.Row
            ledger.execute('PRAGMA query_only=ON')
        request = json.loads(sys.stdin.buffer.read(), object_pairs_hook=pairs)
        print(json.dumps(run(review, src, request, cfg=cfg, ledger=ledger), separators=(',', ':'), ensure_ascii=False))
        return 0
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error, subprocess.SubprocessError) as e:
        print(json.dumps({'status': 'error', 'error': str(e)[:200], 'data_only': True}))
        return 1
    finally:
        for db in (src, review, ledger):
            if db is not None:
                db.close()


if __name__ == '__main__':
    sys.exit(main())
