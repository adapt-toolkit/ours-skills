#!/usr/bin/env python3
"""Durable agent self-wakes. This module has no outbound messaging transport."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
import time
import uuid
from zoneinfo import ZoneInfo


def private_file(path, create=False):
    path = Path(path)
    if not path.is_absolute() or path.parent.is_symlink():
        raise ValueError('absolute private path required')
    parent = path.parent.stat()
    if parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) != 0o700:
        raise ValueError('private directory mode700 required')
    if create:
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            os.close(fd)
        except FileExistsError:
            pass
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
        raise ValueError('private regular file mode600 required')
    return path


def load_config(path):
    cfg = json.loads(private_file(path).read_text())
    required = {'database', 'agent', 'expected_agent_cid', 'agent_state_dir', 'fleet_control_module'}
    if not isinstance(cfg, dict) or not required <= cfg.keys() or cfg.keys() - required - {'node'}:
        raise ValueError('self-wake config fields only; no outbound destination')
    if any(not isinstance(cfg[k], str) or not cfg[k].strip() for k in cfg):
        raise ValueError('nonempty config strings required')
    if not re.fullmatch(r'[A-Za-z0-9_-]+', cfg['agent']):
        raise ValueError('invalid agent name')
    if not re.fullmatch(r'[A-Fa-f0-9]{64}', cfg['expected_agent_cid']):
        raise ValueError('invalid owning-agent identity')
    for key in ('database', 'agent_state_dir', 'fleet_control_module'):
        if not Path(cfg[key]).is_absolute():
            raise ValueError('absolute configured paths required')
    return cfg


def instant(local_time, zone, fold=None):
    wall = datetime.fromisoformat(local_time)
    if wall.tzinfo is not None or 'T' not in local_time:
        raise ValueError('explicit local calendar date/time without offset required')
    tz = ZoneInfo(zone)
    options = {}
    for f in (0, 1):
        dt = wall.replace(tzinfo=tz, fold=f).astimezone(timezone.utc)
        if dt.astimezone(tz).replace(tzinfo=None) == wall:
            options[f] = dt.timestamp()
    if not options:
        raise ValueError('nonexistent local time')
    if len(set(options.values())) > 1 and fold is None:
        raise ValueError('ambiguous local time requires explicit fold')
    if fold is not None and (type(fold) is not int or fold not in (0, 1)):
        raise ValueError('invalid fold')
    return options[0 if fold is None else fold]


class Store:
    def __init__(self, path):
        self.path = private_file(path, create=True)
        self.db = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS self_reminders(
            id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL,
            original_request TEXT NOT NULL, revision INTEGER NOT NULL,
            title TEXT NOT NULL, context TEXT NOT NULL,
            local_time TEXT NOT NULL, timezone TEXT NOT NULL, fold INTEGER,
            due REAL NOT NULL, expires REAL NOT NULL, state TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS self_events(
            id TEXT PRIMARY KEY, reminder_id TEXT NOT NULL REFERENCES self_reminders(id),
            revision INTEGER NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            retry_at REAL NOT NULL DEFAULT 0, prompt_id TEXT,
            UNIQUE(reminder_id, revision));
        ''')

    @contextmanager
    def atomic(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def reminder(self, rid):
        row = self.db.execute('SELECT * FROM self_reminders WHERE id=?', (rid,)).fetchone()
        if row is None:
            raise ValueError('unknown reminder')
        return dict(row)

    def schedule(self, args, now, previous=None):
        allowed = {'title', 'context', 'local_time', 'timezone', 'fold', 'late_window_seconds'}
        if set(args) - allowed:
            raise ValueError('invalid schedule fields')
        data = {**(previous or {}), **args}
        for k in ('title', 'local_time', 'timezone'):
            if not isinstance(data.get(k), str) or not data[k].strip() or '\x00' in data[k]:
                raise ValueError('invalid schedule')
        context = data.get('context', '')
        if not isinstance(context, str) or '\x00' in context:
            raise ValueError('invalid context')
        late = args.get('late_window_seconds', previous['expires'] - previous['due'] if previous else 3600)
        if type(late) not in (int, float) or not math.isfinite(late) or not 1 <= late <= 604800:
            raise ValueError('late window must be 1..604800 seconds')
        due = instant(data['local_time'], data['timezone'], data.get('fold'))
        if due <= now:
            raise ValueError('schedule must be in future')
        return (data['title'], context, data['local_time'], data['timezone'], data.get('fold'), due, due + late)

    def create(self, args, now=None):
        now = time.time() if now is None else now
        data = dict(args)
        key = data.pop('request_key', None)
        if not isinstance(key, str) or not key.strip():
            raise ValueError('stable request_key required')
        original = json.dumps(data, sort_keys=True, ensure_ascii=False, allow_nan=False)
        with self.atomic():
            old = self.db.execute('SELECT * FROM self_reminders WHERE request_key=?', (key,)).fetchone()
            if old:
                if old['original_request'] != original:
                    raise ValueError('request key conflict')
                return dict(old)
            values = self.schedule(data, now)
            rid = str(uuid.uuid4())
            self.db.execute('INSERT INTO self_reminders VALUES (?,?,?,1,?,?,?,?,?,?,?,?)',
                            (rid, key, original, *values, 'active'))
            return self.reminder(rid)

    def change(self, args, cancel=False, now=None):
        data = dict(args)
        rid, revision = data.pop('id'), data.pop('expected_revision')
        with self.atomic():
            old = self.reminder(rid)
            if type(revision) is not int or revision != old['revision'] or old['state'] != 'active':
                raise ValueError('stale or terminal reminder')
            if cancel:
                if data:
                    raise ValueError('cancel takes id and expected_revision only')
                self.db.execute("UPDATE self_reminders SET revision=revision+1,state='cancelled' WHERE id=?", (rid,))
            else:
                values = self.schedule(data, time.time() if now is None else now, old)
                self.db.execute('''UPDATE self_reminders SET revision=revision+1,title=?,context=?,local_time=?,
                  timezone=?,fold=?,due=?,expires=? WHERE id=?''', (*values, rid))
            return self.reminder(rid)

    def inspect(self, event_id, now=None):
        e = self.db.execute('SELECT * FROM self_events WHERE id=?', (event_id,)).fetchone()
        if e is None:
            raise ValueError('unknown event')
        e = dict(e)
        r = self.reminder(e['reminder_id'])
        now = time.time() if now is None else now
        eligible = r['state'] == 'active' and r['revision'] == e['revision'] and r['due'] <= now < r['expires']
        return {'event': e, 'reminder': r, 'eligible': eligible, 'data_only': True}

    def complete(self, args, now=None):
        if set(args) != {'event_id', 'expected_revision'}:
            raise ValueError('complete takes event_id and expected_revision')
        with self.atomic():
            v = self.inspect(args['event_id'], now)
            if type(args['expected_revision']) is not int or args['expected_revision'] != v['event']['revision']:
                raise ValueError('stale completion')
            if v['event']['state'] == 'handled':
                return {'event_id': args['event_id'], 'state': 'handled'}
            if not v['eligible'] or v['event']['state'] not in ('submitting', 'queued', 'uncertain'):
                raise ValueError('event no longer eligible or not admitted')
            self.db.execute("UPDATE self_events SET state='handled' WHERE id=?", (args['event_id'],))
            self.db.execute("UPDATE self_reminders SET state='handled' WHERE id=?", (v['reminder']['id'],))
            return {'event_id': args['event_id'], 'state': 'handled'}

    def recover(self):
        # A crash may have happened after queue acceptance. Never blindly re-admit.
        self.db.execute("UPDATE self_events SET state='uncertain' WHERE state='submitting'")

    def tick(self, adapter, now=None):
        now = time.time() if now is None else now
        with self.atomic():
            self.db.execute("UPDATE self_reminders SET state='expired' WHERE state='active' AND expires<=?", (now,))
            for r in self.db.execute('''SELECT id,revision FROM self_reminders r WHERE state='active' AND due<=?
               AND NOT EXISTS(SELECT 1 FROM self_events e WHERE e.reminder_id=r.id AND e.revision=r.revision)
               ORDER BY due,id LIMIT 25''', (now,)).fetchall():
                self.db.execute("INSERT INTO self_events(id,reminder_id,revision,state) VALUES(?,?,?,'pending')",
                                (str(uuid.uuid4()), r['id'], r['revision']))
        output = []
        candidates = self.db.execute("SELECT id FROM self_events WHERE state='pending' AND retry_at<=? LIMIT 25", (now,)).fetchall()
        for candidate in candidates:
            eid = candidate['id']
            with self.atomic():
                v = self.inspect(eid, now)
                if not v['eligible']:
                    self.db.execute("UPDATE self_events SET state='invalidated' WHERE id=?", (eid,))
                    continue
                changed = self.db.execute("UPDATE self_events SET state='submitting',attempts=attempts+1 WHERE id=? AND state='pending'", (eid,))
                if changed.rowcount != 1:
                    continue
            try:
                outcome = adapter({'event_id': eid, 'revision': v['event']['revision']})
            except Exception:
                outcome = {'status': 'uncertain'}
            with self.atomic():
                current = self.db.execute('SELECT * FROM self_events WHERE id=?', (eid,)).fetchone()
                # The agent can complete before submit_prompt returns its receipt.
                if current['state'] == 'handled':
                    continue
                status = outcome.get('status')
                prompt = outcome.get('prompt_id')
                if status == 'queued' and isinstance(prompt, str) and prompt:
                    state = 'queued'
                elif status == 'unavailable':
                    state = 'pending' if current['attempts'] < 3 else 'failed'
                elif status == 'rejected':
                    state = 'failed'
                else:
                    state = 'uncertain'
                self.db.execute('UPDATE self_events SET state=?,prompt_id=?,retry_at=? WHERE id=?',
                                (state, prompt, now + 30 * 2 ** (current['attempts'] - 1), eid))
                output.append({'event_id': eid, 'state': state})
        return output

    def next_delay(self, now=None):
        now = time.time() if now is None else now
        r = self.db.execute('''SELECT MIN(due) FROM self_reminders r WHERE state='active'
          AND NOT EXISTS(SELECT 1 FROM self_events e WHERE e.reminder_id=r.id AND e.revision=r.revision)''').fetchone()[0]
        e = self.db.execute("SELECT MIN(retry_at) FROM self_events WHERE state='pending'").fetchone()[0]
        values = [x for x in (r, e) if x is not None]
        return max(.1, min(30, min(values) - now)) if values else 30


def wake(cfg, event, config_path):
    result = subprocess.run([cfg.get('node', 'node'), str(Path(__file__).with_name('fleet_wake.mjs'))],
                            input=json.dumps({'config': cfg, 'config_path': str(config_path), 'event': event}),
                            text=True, capture_output=True, timeout=15)
    if result.returncode:
        return {'status': 'uncertain'}
    return json.loads(result.stdout)


def run_worker(store, cfg, path, once=False):
    lock_path = private_file(str(store.path) + '.worker.lock', create=True)
    with lock_path.open('r+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        store.recover()
        while True:
            result = store.tick(lambda event: wake(cfg, event, path))
            if once:
                return result
            time.sleep(store.next_delay())


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser()
    p.add_argument('--config', required=True)
    p.add_argument('action', choices=['check', 'create', 'update', 'cancel', 'list', 'inspect', 'complete', 'tick', 'serve'])
    opts = p.parse_args()
    cfg = load_config(opts.config)
    if opts.action == 'check':
        print(json.dumps({'config': 'valid', 'outbound_destination_required': False}))
        return
    store = Store(cfg['database'])
    if opts.action in ('serve', 'tick'):
        result = run_worker(store, cfg, opts.config, once=opts.action == 'tick')
    else:
        raw = sys.stdin.read()
        args = json.loads(raw) if raw.strip() else {}
        if not isinstance(args, dict):
            raise ValueError('JSON object required')
        if opts.action == 'create':
            result = store.create(args)
        elif opts.action in ('update', 'cancel'):
            result = store.change(args, cancel=opts.action == 'cancel')
        elif opts.action == 'inspect':
            if set(args) != {'event_id'}:
                raise ValueError('inspect takes event_id')
            result = store.inspect(args['event_id'])
        elif opts.action == 'complete':
            result = store.complete(args)
        else:
            if set(args) - {'state', 'after_id', 'limit'}:
                raise ValueError('invalid list filter')
            limit = args.get('limit', 25)
            if type(limit) is not int or not 1 <= limit <= 100:
                raise ValueError('invalid page limit')
            rows = store.db.execute('SELECT * FROM self_reminders WHERE state=? AND id>? ORDER BY id LIMIT ?',
                                    (args.get('state', 'active'), args.get('after_id', ''), limit + 1)).fetchall()
            result = {'items': [dict(x) for x in rows[:limit]], 'more': len(rows) > limit,
                      'after_id': rows[limit-1]['id'] if len(rows) >= limit else None}
    if opts.action != 'serve':
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    store.db.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(json.dumps({'status': 'operation_failed', 'error_type': type(e).__name__}), file=sys.stderr)
        sys.exit(1)
