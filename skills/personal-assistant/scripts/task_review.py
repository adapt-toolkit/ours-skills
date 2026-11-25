#!/usr/bin/env python3
"""Durable periodic reconsideration of open source tasks.

A deterministic scan of the configured schema2 source (opened read-only)
decides which open tasks are eligible for reconsideration and when ONE native
self-reminder should next wake the assistant. Review/send accounting lives in a
separate private sidecar database; the source store, its writer, preferences,
timers and delivery ledgers are never written here. This script sends nothing.
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
 total_sends INTEGER NOT NULL DEFAULT 0,
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
CREATE TABLE review_chain(
 id INTEGER PRIMARY KEY CHECK(id=1),reminder_id TEXT NOT NULL,revision INTEGER NOT NULL,
 request_key TEXT NOT NULL,due_utc TEXT NOT NULL);
'''

# Policy defaults (assistant-selected, documented in references/task-review.md).
MIN_SPACING = timedelta(hours=2)          # between review wakes / per-window send budget
FIRST_GRACE = timedelta(hours=24)         # new task: it was just discussed
CHANGE_GRACE = timedelta(hours=3)         # source change: let the conversation settle
NO_MESSAGE_DEFER = timedelta(hours=24)    # reviewed, nothing useful to say: reconsider daily
FIRST_SPREAD_HOURS = 48                   # stagger first reviews (e.g. activation with many tasks)
COALESCE = timedelta(hours=2)             # pull near-due tasks into the current wake
MAX_REVIEW_PER_WAKE = 20
SEND_BACKOFF = [timedelta(days=1), timedelta(days=2), timedelta(days=4)]
MAX_UNANSWERED = 3                        # then slow reconsideration only
SLOW_RECONSIDER = timedelta(days=14)
MAX_DEFER = timedelta(days=60)
MAX_SENDS_PER_WINDOW = 3                  # send claims per 2h window across tasks
LATE_WINDOW = 604800                      # one bounded catch-up wake after downtime
PLANNING_QUIET = ('21:00', '09:00')       # wake placement only; never authorizes a send
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
    """User-confirmed quiet hours (sidecar, else operator-recorded); otherwise planning only."""
    row = review.execute("SELECT value FROM review_meta WHERE key='quiet_hours'").fetchone()
    if row is not None:
        return json.loads(row[0])
    bound = (cfg or {}).get('task_review_quiet_hours')
    if (isinstance(bound, dict) and bound.get('basis') == 'user_confirmed'
            and isinstance(bound.get('confirmed_wire_id'), str) and bound['confirmed_wire_id']):
        if _hm(bound['start']) == _hm(bound['end']):
            raise ValueError('invalid configured quiet hours')
        return {k: bound[k] for k in ('start', 'end', 'basis', 'confirmed_wire_id')}
    return {'start': PLANNING_QUIET[0], 'end': PLANNING_QUIET[1], 'basis': 'unconfirmed_planning_only'}


def in_quiet(at, zone, quiet):
    local = at.astimezone(zone).time()
    start, end = _hm(quiet['start']), _hm(quiet['end'])
    return (start <= local < end) if start < end else (local >= start or local < end)


def out_of_quiet(at, zone, quiet):
    """Earliest instant >= at outside quiet hours, in the user's zone."""
    if not in_quiet(at, zone, quiet):
        return at
    local = at.astimezone(zone)
    end = _hm(quiet['end'])
    candidate = datetime.combine(local.date(), end, tzinfo=zone)
    if candidate <= local:
        candidate = datetime.combine(local.date() + timedelta(days=1), end, tzinfo=zone)
    return candidate.astimezone(timezone.utc)


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


def legacy_covered(src, task_id):
    """An active legacy source-bound reminder already owns this task's next return."""
    try:
        row = src.execute("SELECT reminder_id FROM reminder_mappings WHERE source_ref=? AND status='active' LIMIT 1",
                          ('task:' + task_id,)).fetchone()
    except sqlite3.Error:
        return None
    return None if row is None else row['reminder_id']


def send_cooldown_until(row):
    """Send spacing from the actual last send; never shortened by source changes or restarts."""
    if row['last_send_utc'] is None or row['unanswered_sends'] == 0:
        return None
    n = row['unanswered_sends']
    wait = SLOW_RECONSIDER if n >= MAX_UNANSWERED else SEND_BACKOFF[min(n - 1, len(SEND_BACKOFF) - 1)]
    return parse(row['last_send_utc']) + wait


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
                               (task_id, t['revision'], int(t['ready']), iso(at), iso(first_due(task_id, at)), 'first_seen'))
            continue
        if not live:
            if row['closed_status'] is None:
                review.execute('UPDATE task_reviews SET closed_status=?,reason=? WHERE task_id=?',
                               ('archived' if t['archived'] else t['status'], 'source_closed', task_id))
            continue
        if row['closed_status'] is not None:  # explicitly reopened in source
            review.execute('UPDATE task_reviews SET closed_status=NULL WHERE task_id=?', (task_id,))
        if t['revision'] != row['seen_revision'] or int(t['ready']) != row['seen_ready']:
            # A meaningful change may bring REVIEW forward; send budget/cooldown untouched.
            target = min(parse(row['next_eligible_utc']), at + CHANGE_GRACE)
            why = 'became_ready' if t['ready'] and not row['seen_ready'] else 'source_changed'
            review.execute('UPDATE task_reviews SET seen_revision=?,seen_ready=?,next_eligible_utc=?,reason=? WHERE task_id=?',
                           (t['revision'], int(t['ready']), iso(target), why, task_id))
    return tasks


def first_due(task_id, at):
    spread = int(hashlib.sha256(task_id.encode()).hexdigest()[:8], 16) % (FIRST_SPREAD_HOURS * 60)
    return at + FIRST_GRACE + timedelta(minutes=spread)


def claim_token(row, t):
    state = [row['task_id'], t['revision'], int(t['ready']), row['next_eligible_utc'], row['last_send_utc'],
             row['unanswered_sends'], row['opted_out'], row['in_flight_attempt'], row['covered_until_utc']]
    return hashlib.sha256(json.dumps(state).encode()).hexdigest()[:24]


def classify(src, row, t, at):
    """Deterministic review eligibility of one tracked task now: (state, detail, wait_until)."""
    if t is None:
        return 'held', 'source_missing', None
    if row['opted_out']:
        return 'opted_out', 'user_opted_out', None
    if row['in_flight_attempt']:
        return 'held', 'uncertain_send_reconcile_first', None
    if legacy_covered(src, row['task_id']) is not None:
        return 'held', 'covered_by_existing_timer', None
    due = parse(row['next_eligible_utc'])
    if row['covered_until_utc'] and parse(row['covered_until_utc']) > at:
        # An exact-time timer owns this purpose until it has fired; never pre-empt it.
        return 'waiting', 'covered_by_native_timer', max(due, parse(row['covered_until_utc']))
    if due > at + COALESCE:
        return 'waiting', row['reason'], due
    return 'eligible', row['reason'], None


def plan(review, src, at, cfg=None):
    zone = confirmed_zone(src)
    quiet = quiet_window(review, cfg)
    with source.atomic(review, True):
        tasks = sync(review, src, at)
        rows = review.execute('SELECT * FROM task_reviews WHERE closed_status IS NULL ORDER BY next_eligible_utc,task_id').fetchall()
    eligible, held, waits = [], [], []
    for r in rows:
        t = tasks.get(r['task_id'])
        state, detail, due = classify(src, r, t, at)
        if state == 'eligible':
            cooldown = send_cooldown_until(r)
            eligible.append({'id': r['task_id'], 'title': t['title'], 'revision': t['revision'],
                             'ready': bool(t['ready']), 'reason': detail,
                             'last_review_utc': r['last_review_utc'], 'last_outcome': r['last_outcome'],
                             'last_send_utc': r['last_send_utc'], 'unanswered_sends': r['unanswered_sends'],
                             'send_allowed_after_utc': None if cooldown is None or cooldown <= at else iso(cooldown),
                             'household_guard_required': r['task_id'].startswith('hh-'),
                             'claim_token': claim_token(r, t)})
        elif state == 'waiting':
            waits.append(due)
        elif state == 'held':
            held.append({'id': r['task_id'], 'reason': detail})
    quiet_now = in_quiet(at, zone, quiet)
    batch = [] if quiet_now else eligible[:MAX_REVIEW_PER_WAKE]
    deferred = len(eligible) - len(batch)
    candidates = waits + ([at + MIN_SPACING] if eligible else [])
    chain = review.execute('SELECT * FROM review_chain WHERE id=1').fetchone()
    chain_current = chain is not None and parse(chain['due_utc']) > at
    schedule = None
    if candidates:
        target = max(min(candidates), at + timedelta(minutes=5))
        if chain is not None and not chain_current:
            target = max(target, parse(chain['due_utc']) + MIN_SPACING)
        target = out_of_quiet(target, zone, quiet)
        local = target.astimezone(zone).replace(second=0, microsecond=0)
        if local.astimezone(timezone.utc) < target:
            local = (local + timedelta(minutes=1)).astimezone(timezone.utc).astimezone(zone)
        due_utc = local.astimezone(timezone.utc)
        spec = {'title': 'Reconsider open personal tasks',
                'note': 'task_review predicted eligibility only: run plan first; tasks may have changed.',
                'local_time': local.replace(tzinfo=None).isoformat(timespec='minutes'),
                'time_zone': zone.key, 'late_window_seconds': LATE_WINDOW}
        if local.fold:
            spec['fold'] = 1
        if not chain_current:
            key = 'task-review-' + hashlib.sha256((zone.key + iso(due_utc)).encode()).hexdigest()[:16]
            schedule = {'action': 'create', 'args': spec | {'request_key': key}, 'due_utc': iso(due_utc)}
        elif due_utc < parse(chain['due_utc']) - timedelta(minutes=15):
            args = {'id': chain['reminder_id'], 'expected_revision': chain['revision'],
                    'local_time': spec['local_time'], 'time_zone': zone.key}
            if local.fold:
                args['fold'] = 1
            schedule = {'action': 'update', 'args': args, 'due_utc': iso(due_utc)}
    elif chain_current:
        schedule = {'action': 'cancel', 'args': {'id': chain['reminder_id'], 'expected_revision': chain['revision']}}
    return {'status': 'ok', 'data_only': True, 'now_utc': iso(at), 'time_zone': zone.key,
            'quiet_hours': quiet, 'quiet_now': quiet_now,
            'sends_possible': quiet['basis'] != 'unconfirmed_planning_only' and not quiet_now,
            'eligible': batch, 'eligible_deferred': deferred, 'held': held, 'open_tracked': len(rows),
            'chain': None if chain is None else {'reminder_id': chain['reminder_id'], 'revision': chain['revision'],
                                                 'due_utc': chain['due_utc'], 'current': chain_current},
            'schedule': schedule}


def _task_row(review, task_id):
    row = review.execute('SELECT * FROM task_reviews WHERE task_id=?', (source.identifier(task_id),)).fetchone()
    if row is None:
        raise ValueError('task not tracked; run plan first')
    return row


def _defer(review, task_id, outcome, wait, reason, at, rev, attempt, note, chosen=None):
    nxt = at + wait
    if chosen is not None:
        chosen = parse(chosen)
        if not at + timedelta(minutes=30) <= chosen <= at + MAX_DEFER:
            raise ValueError('next_review_utc must be 30 minutes to 60 days ahead')
        nxt, reason = chosen, reason + ':assistant_selected_time'
    review.execute('UPDATE task_reviews SET last_review_utc=?,last_outcome=?,next_eligible_utc=?,reason=? WHERE task_id=?',
                   (iso(at), outcome, iso(nxt), reason, task_id))
    review.execute('INSERT INTO review_log VALUES (?,?,?,?,?,?)', (attempt, task_id, iso(at), outcome, rev, note))
    return nxt, reason


def record(review, src, data, at, cfg=None):
    """Truthful non-send review decision (or opt-out/opt-in). Idempotent by attempt_id."""
    if set(data) - {'task_id', 'attempt_id', 'outcome', 'note', 'next_review_utc'} or \
            not {'task_id', 'attempt_id', 'outcome'} <= set(data):
        raise ValueError('task_id, attempt_id and outcome required')
    outcome = data['outcome']
    if outcome not in REVIEW_OUTCOMES:
        raise ValueError('review outcome must be no_message|deferred|opt_out|opt_in; sends use begin_send/send_result')
    attempt = text(data['attempt_id'], 128)
    note = None if data.get('note') is None else text(data['note'], 300)
    with source.atomic(review, True):
        row = _task_row(review, data['task_id'])
        prior = review.execute('SELECT * FROM review_log WHERE attempt_id=?', (attempt,)).fetchone()
        if prior is not None:
            if prior['outcome'] == outcome and prior['task_id'] == row['task_id']:
                return {'status': 'already_recorded', 'attempt_id': attempt}
            raise ValueError('attempt_id already used')
        t = open_tasks(src).get(row['task_id'])
        rev = t['revision'] if t else row['seen_revision']
        if outcome == 'opt_out':
            review.execute('UPDATE task_reviews SET opted_out=1 WHERE task_id=?', (row['task_id'],))
            nxt, reason = _defer(review, row['task_id'], outcome, MAX_DEFER, 'user_opted_out', at, rev, attempt, note)
        elif outcome == 'opt_in':
            review.execute('UPDATE task_reviews SET opted_out=0 WHERE task_id=?', (row['task_id'],))
            nxt, reason = _defer(review, row['task_id'], outcome, timedelta(0), 'user_opted_in', at, rev, attempt, note)
        else:
            if row['in_flight_attempt']:
                raise ValueError('reconcile the in-flight send claim first')
            wait = NO_MESSAGE_DEFER if outcome == 'no_message' else MIN_SPACING
            nxt, reason = _defer(review, row['task_id'], outcome, wait, outcome, at, rev, attempt, note,
                                 data.get('next_review_utc'))
    return {'status': 'recorded', 'attempt_id': attempt, 'task_id': row['task_id'], 'outcome': outcome,
            'next_eligible_utc': iso(nxt), 'reason': reason}


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


def begin_send(review, src, data, at, cfg=None):
    """Durable CAS claim BEFORE any outward review message; every gate rechecked at actual time."""
    if set(data) != {'task_id', 'attempt_id', 'source_revision', 'claim_token'}:
        raise ValueError('task_id, attempt_id, source_revision and claim_token from plan required')
    attempt = text(data['attempt_id'], 128)
    prior = review.execute('SELECT * FROM send_claims WHERE attempt_id=?', (attempt,)).fetchone()
    if prior is not None:
        # Replay never re-authorizes: an existing claim must be closed via send_result/reconcile.
        return {'status': 'already_claimed', 'attempt_id': attempt, 'state': prior['state'], 'send': False}
    quiet = quiet_window(review, cfg)
    if quiet['basis'] == 'unconfirmed_planning_only':
        raise ValueError('quiet hours not confirmed by the user; review messages held')
    zone = confirmed_zone(src)
    if in_quiet(at, zone, quiet):
        raise ValueError('inside confirmed quiet hours; send held')
    row = _task_row(review, data['task_id'])
    t = open_tasks(src).get(row['task_id'])
    if t is None or t['status'] != 'open' or t['archived'] or row['closed_status']:
        raise ValueError('task is not open in source')
    if row['in_flight_attempt']:
        raise ValueError('another send claim is in flight; reconcile it first')
    if t['revision'] != data['source_revision'] or claim_token(row, t) != data['claim_token']:
        raise ValueError('stale claim; run plan again')
    state, detail, _ = classify(src, row, t, at)
    if state != 'eligible':
        raise ValueError('task not eligible: ' + detail)
    cooldown = send_cooldown_until(row)
    if cooldown is not None and cooldown > at:
        raise ValueError('send cooldown active until ' + iso(cooldown))
    if review.execute('SELECT COUNT(*) FROM send_claims WHERE claimed_utc>?', (iso(at - MIN_SPACING),)).fetchone()[0] >= MAX_SENDS_PER_WINDOW:
        raise ValueError('review send budget for this window used')
    guard = household_guard(cfg, row['task_id'], attempt) if row['task_id'].startswith('hh-') else None
    with source.atomic(review, True):
        current = _task_row(review, row['task_id'])
        if claim_token(current, t) != data['claim_token']:
            raise ValueError('stale claim; run plan again')
        review.execute('UPDATE task_reviews SET in_flight_attempt=? WHERE task_id=?', (attempt, row['task_id']))
        review.execute('INSERT INTO send_claims(attempt_id,task_id,claimed_utc,source_revision,guard_json,state) VALUES (?,?,?,?,?,?)',
                       (attempt, row['task_id'], iso(at), t['revision'], None if guard is None else json.dumps(guard), 'claimed'))
    return {'status': 'claimed', 'attempt_id': attempt, 'task_id': row['task_id'], 'send': True, 'household_guard': guard}


def send_result(review, data, at, cfg=None):
    """Record the ACTUAL transport outcome of a claimed attempt. `uncertain` keeps it held."""
    if set(data) - {'attempt_id', 'outcome', 'wire_id', 'next_review_utc'} or not {'attempt_id', 'outcome'} <= set(data):
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
        wire = text(data['wire_id'], 256) if outcome == 'sent' else None
        if outcome == 'uncertain':
            review.execute("UPDATE send_claims SET state='uncertain' WHERE attempt_id=?", (claim['attempt_id'],))
            review.execute("UPDATE task_reviews SET last_send_outcome='uncertain',reason='uncertain_send_reconcile_first' WHERE task_id=?",
                           (row['task_id'],))
            return {'status': 'held_uncertain', 'attempt_id': claim['attempt_id'],
                    'action': 'check your own outbound history, then send_result sent(+wire_id) or not_sent'}
        review.execute('UPDATE send_claims SET state=?,wire_id=?,closed_utc=? WHERE attempt_id=?',
                       (outcome, wire, iso(at), claim['attempt_id']))
        if outcome == 'sent':
            sent_at = parse(claim['claimed_utc'])
            n = row['unanswered_sends'] + 1
            wait = SLOW_RECONSIDER if n >= MAX_UNANSWERED else SEND_BACKOFF[min(n - 1, len(SEND_BACKOFF) - 1)]
            reason = 'repeat_budget_exhausted_slow_reconsider' if n >= MAX_UNANSWERED else 'awaiting_response_backoff'
            nxt = sent_at + wait
            review.execute('''UPDATE task_reviews SET last_review_utc=?,last_outcome='sent',last_send_utc=?,last_send_wire=?,
                last_send_outcome='sent',unanswered_sends=?,total_sends=total_sends+1,in_flight_attempt=NULL,
                next_eligible_utc=?,reason=? WHERE task_id=?''',
                           (iso(at), iso(sent_at), wire, n, iso(nxt), reason, row['task_id']))
        else:
            nxt, reason = at + MIN_SPACING, 'send_' + outcome
            review.execute('''UPDATE task_reviews SET last_review_utc=?,last_outcome=?,last_send_outcome=?,in_flight_attempt=NULL,
                next_eligible_utc=?,reason=? WHERE task_id=?''', (iso(at), outcome, outcome, iso(nxt), reason, row['task_id']))
    return {'status': 'recorded', 'attempt_id': claim['attempt_id'], 'task_id': row['task_id'], 'outcome': outcome,
            'next_eligible_utc': iso(nxt), 'reason': reason}


def responded(review, data, at, cfg=None):
    """The user actually engaged with this task in an incoming message: clears unanswered count only."""
    if set(data) != {'task_id', 'wire_id'}:
        raise ValueError('task_id and actual incoming wire_id required')
    text(data['wire_id'], 256)
    with source.atomic(review, True):
        row = _task_row(review, data['task_id'])
        review.execute('''UPDATE task_reviews SET unanswered_sends=0,reason='user_responded',
            next_eligible_utc=MAX(next_eligible_utc,?) WHERE task_id=?''', (iso(at + CHANGE_GRACE), row['task_id']))
    return {'status': 'recorded', 'task_id': row['task_id']}


def cover(review, data, at, cfg=None):
    """Register a native exact-time self-reminder that already covers this task's next return."""
    if set(data) - {'task_id', 'reminder_id', 'due_utc', 'clear'} or 'task_id' not in data:
        raise ValueError('task_id with reminder_id and due_utc, or clear:true, required')
    with source.atomic(review, True):
        row = _task_row(review, data['task_id'])
        if data.get('clear') is True:
            review.execute('UPDATE task_reviews SET covered_by=NULL,covered_until_utc=NULL WHERE task_id=?', (row['task_id'],))
            return {'status': 'cleared', 'task_id': row['task_id']}
        due = parse(data['due_utc'])
        if due <= at or due > at + timedelta(days=1830):
            raise ValueError('future due_utc required')
        until = due + timedelta(hours=1)
        review.execute('UPDATE task_reviews SET covered_by=?,covered_until_utc=? WHERE task_id=?',
                       (text(data['reminder_id'], 128), iso(until), row['task_id']))
    return {'status': 'covered', 'task_id': row['task_id'], 'until_utc': iso(until)}


def chain(review, data, at, cfg=None):
    """Record the receipt of the ONE review self-reminder actually created/updated (or cancelled)."""
    if data == {'cancelled': True}:
        with source.atomic(review, True):
            review.execute('DELETE FROM review_chain WHERE id=1')
        return {'status': 'chain_cleared'}
    if set(data) != {'reminder_id', 'revision', 'request_key', 'due_at'}:
        raise ValueError('actual fleet reminder id, revision, request_key and due_at required')
    if type(data['revision']) is not int or data['revision'] < 1:
        raise ValueError('positive revision required')
    due = datetime.fromisoformat(text(data['due_at'], 64).replace('Z', '+00:00'))
    if due.utcoffset() is None:
        raise ValueError('due_at with offset required')
    with source.atomic(review, True):
        review.execute('INSERT OR REPLACE INTO review_chain VALUES (1,?,?,?,?)',
                       (text(data['reminder_id'], 128), data['revision'], text(data['request_key'], 128), iso(due)))
    return {'status': 'chain_recorded', 'due_utc': iso(due)}


def set_quiet_hours(review, data, at, cfg=None):
    if set(data) != {'start', 'end', 'confirmed_wire_id'}:
        raise ValueError('start, end (HH:MM) and the user confirmation wire required')
    if _hm(data['start']) == _hm(data['end']):
        raise ValueError('empty or full-day quiet window unsupported')
    value = {'start': data['start'], 'end': data['end'], 'basis': 'user_confirmed',
             'confirmed_wire_id': text(data['confirmed_wire_id'], 256), 'confirmed_at_utc': iso(at)}
    with source.atomic(review, True):
        review.execute("INSERT OR REPLACE INTO review_meta VALUES ('quiet_hours',?)", (json.dumps(value),))
    return {'status': 'quiet_hours_set', 'quiet_hours': value}


def status(review, data, at, cfg=None):
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
    chain_row = review.execute('SELECT reminder_id,revision,due_utc FROM review_chain WHERE id=1').fetchone()
    return {'status': 'ok', 'counts': counts, 'quiet_hours': quiet_window(review, cfg),
            'chain': None if chain_row is None else dict(chain_row) | {'current': parse(chain_row['due_utc']) > at},
            'repair_needed': counts.get('tracked', 0) + counts.get('in_flight', 0) > 0
                             and (chain_row is None or parse(chain_row['due_utc']) <= at)}


WITH_SOURCE = {'begin_send': begin_send, 'record': record}
REVIEW_ONLY = {'send_result': send_result, 'responded': responded, 'cover': cover, 'chain': chain,
               'set_quiet_hours': set_quiet_hours, 'status': status}


def run(review, src, request, at=None, cfg=None):
    at = at or now_utc()
    if not isinstance(request, dict) or set(request) - {'action', 'args'} or 'action' not in request:
        raise ValueError('{"action":...,"args":{...}} required')
    action, args = request['action'], request.get('args', {})
    if not isinstance(args, dict):
        raise ValueError('args object required')
    if action == 'plan':
        if args:
            raise ValueError('plan takes no arguments')
        return plan(review, src, at, cfg)
    if action in WITH_SOURCE:
        return WITH_SOURCE[action](review, src, args, at, cfg)
    if action in REVIEW_ONLY:
        return REVIEW_ONLY[action](review, args, at, cfg)
    raise ValueError('unsupported action')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', required=True)
    p.add_argument('--init', action='store_true', help='operator: create the empty sidecar once')
    opts = p.parse_args()
    src = review = None
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
        request = json.loads(sys.stdin.buffer.read(), object_pairs_hook=pairs)
        print(json.dumps(run(review, src, request, cfg=cfg), separators=(',', ':'), ensure_ascii=False))
        return 0
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error, subprocess.SubprocessError) as e:
        print(json.dumps({'status': 'error', 'error': str(e)[:200], 'data_only': True}))
        return 1
    finally:
        for db in (src, review):
            if db is not None:
                db.close()


if __name__ == '__main__':
    sys.exit(main())
