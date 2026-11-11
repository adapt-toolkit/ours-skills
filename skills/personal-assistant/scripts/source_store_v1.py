#!/usr/bin/env python3
"""Private source records and prerequisite graph. Never schedules or sends messages."""
import argparse
import hashlib
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


def resolve_local(local, zone, fold=None):
    naive = datetime.fromisoformat(local)
    if naive.tzinfo is not None:
        raise ValueError('local time must have no offset; provide IANA timezone separately')
    if 'T' not in local or len(local) < 16:
        raise ValueError('explicit local date and time required')
    if fold is not None and (type(fold) is not int or fold not in (0, 1)):
        raise ValueError('fold must be 0 or 1')
    tz = ZoneInfo(zone)
    valid = {}
    for candidate in (0, 1):
        aware = naive.replace(tzinfo=tz, fold=candidate)
        utc = aware.astimezone(timezone.utc)
        if utc.astimezone(tz).replace(tzinfo=None) == naive:
            valid[candidate] = utc
    if not valid:
        raise ValueError('nonexistent local time during DST transition; choose a valid time')
    if len(set(valid.values())) > 1 and fold is None:
        raise ValueError('ambiguous local time; choose fold 0 (first) or 1 (second)')
    return valid[fold if fold is not None else min(valid)]


def stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def connect(path, initialize=False):
    path = Path(path)
    if not initialize and not path.is_file():
        raise ValueError('source store does not exist; initialize a fresh user store first')
    if initialize:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(path, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA synchronous=FULL')
    if initialize:
        db.executescript('''
          CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS tasks(
            id TEXT PRIMARY KEY, title TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('open','completed','cancelled')),
            source TEXT NOT NULL, updated_utc TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS prerequisites(
            task_id TEXT REFERENCES tasks(id), prerequisite_id TEXT REFERENCES tasks(id),
            PRIMARY KEY(task_id, prerequisite_id), CHECK(task_id != prerequisite_id));
          CREATE TABLE IF NOT EXISTS events(
            id TEXT PRIMARY KEY, title TEXT NOT NULL, local TEXT NOT NULL,
            timezone TEXT NOT NULL, fold INTEGER, start_utc TEXT NOT NULL,
            effort_minutes INTEGER NOT NULL, buffer_minutes INTEGER NOT NULL,
            prepare_utc TEXT NOT NULL, source TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('active','cancelled')),
            updated_utc TEXT NOT NULL);
        ''')
        db.execute('INSERT OR IGNORE INTO metadata VALUES (?,?)', ('namespace', str(uuid.uuid4())))
        db.commit()
    return db


def identifier(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError('id must be a nonempty string of at most 128 characters')
    return value


def text_field(data, field):
    value = data[field]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(field + ' must be a concise nonempty string')
    return value


def ready(db):
    return {r[0] for r in db.execute('''SELECT id FROM tasks t WHERE status='open'
      AND NOT EXISTS (SELECT 1 FROM prerequisites p JOIN tasks q ON q.id=p.prerequisite_id
      WHERE p.task_id=t.id AND q.status != 'completed')''')}


def check_dag(db):
    edges = {}
    for a, b in db.execute('SELECT task_id,prerequisite_id FROM prerequisites'):
        edges.setdefault(a, []).append(b)
    active, done = set(), set()
    for root in edges:
        stack = [(root, False)]
        while stack:
            node, exiting = stack.pop()
            if exiting:
                active.remove(node); done.add(node)
            elif node in active:
                raise ValueError('prerequisite cycle rejected')
            elif node not in done:
                active.add(node); stack.append((node, True))
                stack.extend((parent, False) for parent in edges.get(node, []))


def task_record(db, key):
    row = db.execute('SELECT * FROM tasks WHERE id=?', (key,)).fetchone()
    if row is None:
        raise ValueError('unknown task id')
    result = dict(row)
    result['prerequisites'] = [r[0] for r in db.execute(
        'SELECT prerequisite_id FROM prerequisites WHERE task_id=? ORDER BY prerequisite_id', (key,))]
    result['blocked_by'] = [r[0] for r in db.execute('''SELECT q.id FROM prerequisites p
        JOIN tasks q ON q.id=p.prerequisite_id WHERE p.task_id=? AND q.status!='completed'
        ORDER BY q.id''', (key,))]
    result['ready'] = result['status'] == 'open' and not result['blocked_by']
    return result


def context_snapshot(db, ref, max_nodes=None):
    """Recover one bounded source graph, without executing or authorizing anything."""
    if not isinstance(ref, str) or ':' not in ref:
        raise ValueError('source reference must be task:<id> or event:<id>')
    kind, key = ref.split(':', 1)
    identifier(key)
    if kind not in ('task', 'event'):
        raise ValueError('source reference must be task:<id> or event:<id>')
    if max_nodes is not None and (type(max_nodes) is not int or max_nodes < 1):
        raise ValueError('legacy max_nodes hint must be positive')
    namespace = db.execute("SELECT value FROM metadata WHERE key='namespace'").fetchone()[0]
    if kind == 'event':
        row = db.execute('SELECT * FROM events WHERE id=?', (key,)).fetchone()
        if row is None:
            raise ValueError('unknown event id')
        records = [dict(row)]
        dependents = []
    else:
        # Include direct successors so recovery can explain next steps; ancestors
        # of every included task reveal all blockers without scanning other tasks.
        dependents = [r[0] for r in db.execute(
            'SELECT task_id FROM prerequisites WHERE prerequisite_id=? ORDER BY task_id', (key,))]
        pending, found = [key] + dependents, {}
        while pending:
            current = pending.pop()
            if current in found:
                continue
            record = task_record(db, current)
            found[current] = record
            pending.extend(record['prerequisites'])
        records = [found[k] for k in sorted(found)]
    snapshot = {'namespace': namespace, 'ref': ref, 'kind': kind,
                'root_id': key, 'records': records, 'dependent_ids': dependents}
    root = next(r for r in records if r['id'] == key)
    snapshot['sendable'] = root['status'] == ('open' if kind == 'task' else 'active')
    encoded = json.dumps(snapshot, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    return {**snapshot, 'fingerprint': hashlib.sha256(encoded).hexdigest(), 'data_only': True}


def read_context(path, ref, max_nodes=None):
    """Read-only API for a configured reminder wrapper; no implicit initialization."""
    path = Path(path)
    if not path.is_file() or path.is_symlink():
        raise ValueError('configured source store must exist and may not be a symlink')
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        return context_snapshot(db, ref, max_nodes)
    finally:
        db.close()


def run(db, action, data):
    now = stamp(datetime.now(timezone.utc))
    if action == 'init':
        return {'namespace': db.execute("SELECT value FROM metadata WHERE key='namespace'").fetchone()[0]}
    if action == 'task_put':
        key = identifier(data['id'])
        deps = data.get('prerequisites', [r[0] for r in db.execute(
            'SELECT prerequisite_id FROM prerequisites WHERE task_id=?', (key,))])
        if not isinstance(deps, list) or len(deps) != len(set(deps)):
            raise ValueError('prerequisites must be a list of distinct task IDs')
        for parent in deps:
            identifier(parent)
            if db.execute('SELECT 1 FROM tasks WHERE id=?', (parent,)).fetchone() is None:
                raise ValueError('unknown prerequisite; create an explicit source task first')
        db.execute('''INSERT INTO tasks VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE
            SET title=excluded.title,source=excluded.source,updated_utc=excluded.updated_utc''',
            (key, text_field(data, 'title'), 'open', text_field(data, 'source'), now))
        db.execute('DELETE FROM prerequisites WHERE task_id=?', (key,))
        db.executemany('INSERT INTO prerequisites VALUES (?,?)', [(key, p) for p in deps])
        check_dag(db)
        return task_record(db, key)
    if action == 'task_status':
        key = identifier(data['id'])
        before = ready(db)
        task_record(db, key)
        if data['status'] not in ('open', 'completed', 'cancelled'):
            raise ValueError('invalid task status')
        db.execute('UPDATE tasks SET status=?,source=?,updated_utc=? WHERE id=?',
                   (data['status'], text_field(data, 'source'), now, key))
        return {'task': task_record(db, key), 'newly_ready': sorted(ready(db) - before)}
    if action == 'task_get':
        return task_record(db, identifier(data['id']))
    if action == 'context':
        if set(data) - {'ref', 'max_nodes'}:
            raise ValueError('context takes only ref and optional max_nodes')
        return context_snapshot(db, data['ref'], data.get('max_nodes'))
    if action == 'event_put':
        key = identifier(data['id'])
        start = resolve_local(data['local'], data['timezone'], data.get('fold'))
        effort, buffer = data['effort_minutes'], data['buffer_minutes']
        if any(type(v) is not int or v < 0 or v > 525600 for v in (effort, buffer)):
            raise ValueError('confirmed effort and buffer must be nonnegative integer minutes')
        prepare = start - timedelta(minutes=effort + buffer)
        db.execute('''INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id)
          DO UPDATE SET title=excluded.title,local=excluded.local,timezone=excluded.timezone,
          fold=excluded.fold,start_utc=excluded.start_utc,effort_minutes=excluded.effort_minutes,
          buffer_minutes=excluded.buffer_minutes,prepare_utc=excluded.prepare_utc,
          source=excluded.source,status=excluded.status,updated_utc=excluded.updated_utc''',
          (key, text_field(data, 'title'), data['local'], data['timezone'], data.get('fold'),
           stamp(start), effort, buffer, stamp(prepare), text_field(data, 'source'), 'active', now))
        return dict(db.execute('SELECT * FROM events WHERE id=?', (key,)).fetchone())
    if action == 'event_cancel':
        cursor = db.execute('UPDATE events SET status=\'cancelled\',updated_utc=? WHERE id=?',
                            (now, identifier(data['id'])))
        if cursor.rowcount != 1:
            raise ValueError('unknown event id')
        return {'id': data['id'], 'status': 'cancelled', 'reminder_action_required': True}
    if action == 'list':
        kind = data.get('kind', 'tasks')
        if kind not in ('tasks', 'events'):
            raise ValueError('kind must be tasks or events')
        limit = data.get('limit', 50)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError('limit must be 1..200')
        query = data.get('query', '')
        if not isinstance(query, str):
            raise ValueError('query must be text')
        # instr treats percent/underscore as literal search text.
        rows = db.execute(f'SELECT * FROM {kind} WHERE instr(lower(title),lower(?))>0 '
                          'ORDER BY updated_utc DESC,id LIMIT ?', (query, limit)).fetchall()
        return [task_record(db, r['id']) if kind == 'tasks' else dict(r) for r in rows]
    raise ValueError('unknown action')


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True)
    parser.add_argument('action', choices=('init', 'task_put', 'task_status', 'task_get',
                                         'event_put', 'event_cancel', 'list', 'context'))
    args = parser.parse_args()
    db = None
    try:
        data = json.load(sys.stdin) if args.action != 'init' else {}
        if not isinstance(data, dict):
            raise ValueError('input must be a JSON object')
        db = connect(args.db, args.action == 'init')
        # Serialize graph mutations and take a consistent snapshot for read responses.
        db.execute('BEGIN IMMEDIATE' if args.action not in ('init', 'list', 'task_get', 'context') else 'BEGIN')
        result = run(db, args.action, data)
        db.commit()
        print(json.dumps({'ok': True, 'result': result}, ensure_ascii=False))
    except (ValueError, KeyError, TypeError, sqlite3.Error, OSError, RecursionError) as error:
        if db is not None:
            db.rollback()
        # Do not echo private values or raw input on failures.
        print(json.dumps({'ok': False, 'error': type(error).__name__,
                          'detail': str(error) if isinstance(error, ValueError) else 'invalid input or store operation failed'}))
        return 1
    finally:
        if db is not None:
            db.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
