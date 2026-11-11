#!/usr/bin/env python3
"""Private indexed source v2. Explicit import, revision CAS, bounded data-only recovery."""
import argparse
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import time
import uuid

from context_budget import ContextOverflow, bounded_result, compact_json, fits

SCHEMA_VERSION = 2
TASK_STATUSES = ('open', 'completed', 'cancelled')
EVENT_STATUSES = ('active', 'cancelled')
TERMINAL_REMINDER_STATUSES = ('cancelled', 'expired', 'invalidated', 'transport_confirmed', 'dismissed')
HISTORICAL_BINDINGS_SCHEMA = '''
CREATE TABLE historical_terminal_bindings(
 reminder_id TEXT PRIMARY KEY,source_ref TEXT NOT NULL,reminder_revision INTEGER NOT NULL CHECK(reminder_revision>0),
 status TEXT NOT NULL CHECK(status IN ('cancelled','expired','invalidated','transport_confirmed','dismissed')),
 reason TEXT NOT NULL CHECK(reason IN ('unsupported_reference','source_missing')),
 receipt_id TEXT NOT NULL UNIQUE,receipt_json TEXT NOT NULL,receipt_digest TEXT NOT NULL);
'''
SCHEMA = '''
CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE legacy_partitions(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);
CREATE TABLE events(
 id TEXT PRIMARY KEY,title TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('active','cancelled')),
 start_utc TEXT NOT NULL,prepare_utc TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0),
 archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0,1)),updated_utc TEXT NOT NULL,
 original_json TEXT NOT NULL,original_digest TEXT NOT NULL,ordinal INTEGER UNIQUE);
CREATE TABLE tasks(
 id TEXT PRIMARY KEY,title TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('open','completed','cancelled')),
 revision INTEGER NOT NULL CHECK(revision>0),archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0,1)),
 updated_utc TEXT NOT NULL,original_json TEXT NOT NULL,original_digest TEXT NOT NULL,ordinal INTEGER UNIQUE);
CREATE TABLE prerequisites(
 task_id TEXT NOT NULL REFERENCES tasks(id),prerequisite_id TEXT NOT NULL REFERENCES tasks(id),
 PRIMARY KEY(task_id,prerequisite_id),CHECK(task_id!=prerequisite_id));
CREATE INDEX prerequisite_reverse ON prerequisites(prerequisite_id,task_id);
CREATE INDEX event_status_archive_time_id ON events(status,archived,start_utc,id);
CREATE INDEX event_archive_time_id ON events(archived,start_utc,id);
CREATE INDEX task_status_archive_time_id ON tasks(status,archived,updated_utc,id);
CREATE INDEX task_archive_time_id ON tasks(archived,updated_utc,id);
CREATE TABLE source_revisions(
 kind TEXT NOT NULL CHECK(kind IN ('task','event')),id TEXT NOT NULL,revision INTEGER NOT NULL,
 original_json TEXT NOT NULL,original_digest TEXT NOT NULL,archived INTEGER NOT NULL,
 updated_utc TEXT NOT NULL,PRIMARY KEY(kind,id,revision));
CREATE VIRTUAL TABLE source_fts USING fts5(kind UNINDEXED,id UNINDEXED,title,tokenize='unicode61');
CREATE TABLE fts_documents(rowid INTEGER PRIMARY KEY,kind TEXT NOT NULL,id TEXT NOT NULL,UNIQUE(kind,id));
CREATE TABLE semantic_pointers(
 fact_id TEXT PRIMARY KEY,scope TEXT NOT NULL,source_ref TEXT NOT NULL,
 source_revision INTEGER NOT NULL,source_digest TEXT NOT NULL,as_of TEXT NOT NULL,fact_digest TEXT NOT NULL);
CREATE TABLE curated_notes(
 id TEXT PRIMARY KEY,scope TEXT NOT NULL,revision INTEGER NOT NULL,text TEXT NOT NULL,digest TEXT NOT NULL);
CREATE TABLE source_schedule_operations(
 operation_id TEXT PRIMARY KEY,digest TEXT NOT NULL,source_ref TEXT NOT NULL,
 source_revision INTEGER NOT NULL,source_digest TEXT NOT NULL,request_json TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('prepared','mapped')),result_digest TEXT);
CREATE INDEX source_schedule_pending ON source_schedule_operations(source_ref,status);
CREATE TABLE reminder_mappings(
 reminder_id TEXT PRIMARY KEY,source_ref TEXT NOT NULL,source_revision INTEGER NOT NULL,
 source_digest TEXT NOT NULL,reminder_revision INTEGER NOT NULL,status TEXT NOT NULL,
 operation_id TEXT NOT NULL REFERENCES source_schedule_operations(operation_id));
CREATE INDEX reminder_mapping_source ON reminder_mappings(source_ref,status);
''' + HISTORICAL_BINDINGS_SCHEMA


def digest(value):
    return hashlib.sha256(compact_json(value).encode()).hexdigest()


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def identifier(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 128 or '\x00' in value:
        raise ValueError('invalid source identifier')
    return value


def canonical_namespace(value):
    if not isinstance(value, str) or str(uuid.UUID(value)) != value:
        raise ValueError('canonical source namespace required')
    return value


def private_path(path, create=False):
    path = Path(path)
    if not path.is_absolute():
        raise ValueError('absolute private database path required')
    if create:
        path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    parent = path.parent.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700):
        raise ValueError('private owner mode700 database parent required')
    if create:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(fd)
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600):
        raise ValueError('private owner mode600 regular database required')
    return path


def connect(path, initialize=False, namespace=None, readonly=False, resume_empty=False):
    """Initialization creates a NEW empty path only; never an implicit upgrade."""
    if resume_empty and (initialize or readonly or namespace is None):
        raise ValueError('operator empty-schema resume requires exact namespace and writable existing path')
    path = private_path(path, initialize)
    db = sqlite3.connect(path.as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                         uri=True, isolation_level=None, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    if readonly:
        db.execute('PRAGMA query_only=ON')
    else:
        db.execute('PRAGMA synchronous=FULL')
    try:
        if initialize or resume_empty:
            ns = canonical_namespace(namespace or str(uuid.uuid4()))
            db.execute('BEGIN IMMEDIATE')
            if (db.execute('SELECT 1 FROM sqlite_master LIMIT 1').fetchone()
                    or db.execute('PRAGMA quick_check').fetchone()[0] != 'ok'):
                raise ValueError('initialization requires truly empty intact SQLite schema')
            for statement in SCHEMA.split(';'):
                if statement.strip():
                    db.execute(statement)
            db.executemany('INSERT INTO metadata VALUES (?,?)',
                           [('schema_version', str(SCHEMA_VERSION)), ('namespace', ns),
                            ('generation', '0'), ('legacy_imported', 'false')])
            db.commit()
        check_schema(db)
        return db
    except BaseException:
        if db.in_transaction:
            db.rollback()
        db.close()
        raise


def check_schema(db):
    row = db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
    if row is None or row[0] != str(SCHEMA_VERSION):
        raise ValueError('unsupported source schema; explicit migration required')
    canonical_namespace(meta(db, 'namespace'))


def meta(db, key):
    row = db.execute('SELECT value FROM metadata WHERE key=?', (key,)).fetchone()
    if row is None:
        raise ValueError('source metadata missing')
    return row[0]


@contextmanager
def atomic(db, write=False):
    nested = db.in_transaction
    db.execute('SAVEPOINT source_operation' if nested else 'BEGIN IMMEDIATE' if write else 'BEGIN')
    try:
        yield
        db.execute('RELEASE source_operation') if nested else db.commit()
    except BaseException:
        if nested and db.in_transaction:
            db.execute('ROLLBACK TO source_operation')
            db.execute('RELEASE source_operation')
        elif not nested:
            db.rollback()
        raise


_WORK_BUDGETS = {}

def check_work(db):
    deadline = _WORK_BUDGETS.get(id(db))
    if deadline is not None and time.monotonic() > deadline:
        raise ContextOverflow('source_query_overflow')

@contextmanager
def bounded_work(db, max_steps=None, seconds=1):
    # Nested recovery must share, rather than remove/reset, the outer deadline.
    identity = id(db)
    if identity in _WORK_BUDGETS:
        yield
        return
    # Legacy max_steps is an accounting-era compatibility hint, not a
    # replacement graph/input count ceiling. Finite execution time remains.
    if max_steps is not None and (type(max_steps) is not int or max_steps < 1):
        raise ValueError("legacy max_steps hint must be positive")
    deadline = time.monotonic() + seconds
    _WORK_BUDGETS[identity] = deadline
    def progress():
        return int(time.monotonic() > deadline)
    db.set_progress_handler(progress, 1000)
    try:
        yield
    except sqlite3.OperationalError as e:
        if 'interrupt' in str(e).lower():
            raise ContextOverflow('source_query_overflow') from None
        raise
    finally:
        db.set_progress_handler(None, 0)
        del _WORK_BUDGETS[identity]


def bump(db):
    db.execute("UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='generation'")
    return int(meta(db, 'generation'))


def valid_record(record, kind):
    if not isinstance(record, dict):
        raise ValueError('unsupported or oversized source record')
    identifier(record.get('id'))
    if kind == 'event':
        # Reviewed legacy normalization validates explicit UTC/local agreement,
        # preparation, provenance and statuses; unknown fields remain private.
        import legacy_source
        projected = legacy_source.event_record(record)
        identifier(projected['id'])
        return projected
    for field in ('title', 'source'):
        value = record.get(field)
        if not isinstance(value, str) or not value.strip() or '\x00' in value:
            raise ValueError('invalid explicit task record')
    if record.get('status') not in TASK_STATUSES or record.get('task_schema_version') != 2:
        raise ValueError('explicit task schema2 and status required')
    deps = record.get('prerequisites', [])
    if not isinstance(deps, list):
        raise ValueError('task prerequisite collection exceeds bound')
    if len(set(identifier(v) for v in deps)) != len(deps) or record['id'] in deps:
        raise ValueError('invalid prerequisite references')
    return {k: record[k] for k in ('id', 'title', 'source', 'status', 'task_schema_version')} | {'prerequisites': deps}


def _store_record(db, kind, original, revision, ordinal=None, archived=0):
    r = valid_record(original, kind)
    raw, sha, now = compact_json(original), digest(original), utc_now()
    db.execute('INSERT INTO source_revisions VALUES (?,?,?,?,?,?,?)',
               (kind, r['id'], revision, raw, sha, archived, now))
    if kind == 'event':
        # Normalize sort instants only; export keeps original UTC spelling intact.
        start = datetime.fromisoformat(r['start_utc'][:-1] + '+00:00').isoformat(timespec='microseconds').replace('+00:00', 'Z')
        db.execute('''INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
          title=excluded.title,status=excluded.status,start_utc=excluded.start_utc,
          prepare_utc=excluded.prepare_utc,revision=excluded.revision,archived=excluded.archived,
          updated_utc=excluded.updated_utc,original_json=excluded.original_json,
          original_digest=excluded.original_digest''',
          (r['id'], r['title'], r['status'], start, r['prepare_utc'], revision,
           archived, now, raw, sha, ordinal))
    else:
        db.execute('''INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
          title=excluded.title,status=excluded.status,revision=excluded.revision,archived=excluded.archived,
          updated_utc=excluded.updated_utc,original_json=excluded.original_json,
          original_digest=excluded.original_digest''',
          (r['id'], r['title'], r['status'], revision, archived, now, raw, sha, ordinal))
        db.execute('DELETE FROM prerequisites WHERE task_id=?', (r['id'],))
        for parent in r['prerequisites']:
            if db.execute('SELECT 1 FROM tasks WHERE id=?', (parent,)).fetchone() is None:
                raise ValueError('unknown prerequisite reference')
            db.execute('INSERT INTO prerequisites VALUES (?,?)', (r['id'], parent))
    # UNINDEXED FTS fields cannot support exact maintenance. Resolve stable
    # document rowid through an ordinary B-tree before touching FTS internals.
    document = db.execute('SELECT rowid FROM fts_documents WHERE kind=? AND id=?', (kind,r['id'])).fetchone()
    if document is None:
        document_id = db.execute('INSERT INTO fts_documents(kind,id) VALUES (?,?)', (kind,r['id'])).lastrowid
    else:
        document_id = document[0]
        db.execute('DELETE FROM source_fts WHERE rowid=?', (document_id,))
    db.execute('INSERT INTO source_fts(rowid,kind,id,title) VALUES (?,?,?,?)', (document_id,kind,r['id'],r['title']))
    return r


def import_legacy(db, document, namespace):
    """Operator-only, one transaction. No timers, ledger writes, sends or output records."""
    canonical_namespace(namespace)
    if (not isinstance(document, dict) or type(document.get('schema_version')) is not int
            or document['schema_version'] != 1
            or any(not isinstance(document.get(k), list) for k in ('tasks', 'events', 'reminders'))):
        raise ValueError('unsupported legacy schema')
    if document['tasks']:
        raise ValueError('legacy task schema unqualified; explicit contract required')
    with atomic(db, True):
        check_schema(db)
        if (meta(db, 'namespace') != namespace or meta(db, 'generation') != '0'
                or meta(db, 'legacy_imported') != 'false'
                or db.execute('SELECT 1 FROM events LIMIT 1').fetchone()
                or db.execute('SELECT 1 FROM tasks LIMIT 1').fetchone()):
            raise ValueError('import requires fresh matching-namespace store')
        seen = set()
        for ordinal, original in enumerate(document['events']):
            r = valid_record(original, 'event')
            if r['id'] in seen:
                raise ValueError('duplicate legacy event IDs')
            seen.add(r['id'])
            _store_record(db, 'event', original, 1, ordinal)
        # Preserve all top-level fields/partitions, even those not understood by
        # the ordinary agent tool. None are returned by a recovery/list API.
        db.executemany('INSERT INTO legacy_partitions VALUES (?,?)',
                       [(k, compact_json(v)) for k, v in document.items() if k not in ('tasks', 'events')])
        db.executemany('INSERT OR REPLACE INTO metadata VALUES (?,?)',
                       [('legacy_imported', 'true'), ('import_canonical_digest', digest(document)),
                        ('import_schema', 'legacy1'), ('import_generation', '1')])
        bump(db)
        if export_legacy(db) != document:
            raise ValueError('lossless import comparison failed')
    return metadata(db)


def export_legacy(db):
    """Private operator API ONLY; never an ordinary model-facing tool response."""
    with atomic(db):
        check_schema(db)
        result = {r['key']: json.loads(r['value_json']) for r in db.execute('SELECT * FROM legacy_partitions')}
        result.setdefault('schema_version', 1)
        result.setdefault('reminders', [])
        for kind in ('tasks', 'events'):
            result[kind] = [json.loads(r[0]) for r in db.execute(
                f'SELECT original_json FROM {kind} ORDER BY ordinal,id')]
        return result


def metadata(db):
    with atomic(db):
        check_schema(db)
        return {'schema_version': SCHEMA_VERSION, 'namespace': meta(db, 'namespace'),
                'generation': int(meta(db, 'generation')), 'legacy_imported': meta(db, 'legacy_imported') == 'true',
                'event_count': db.execute('SELECT count(*) FROM events').fetchone()[0],
                'task_count': db.execute('SELECT count(*) FROM tasks').fetchone()[0],
                'export_canonical_digest': digest(export_legacy(db)), 'data_only': True}


def _row(db, kind, key):
    r = db.execute(f'SELECT * FROM {kind}s WHERE id=?', (identifier(key),)).fetchone()
    if r is None:
        raise ValueError('unknown source reference')
    return r


def _task(db, key):
    row = _row(db, 'task', key)
    deps = db.execute('''SELECT p.prerequisite_id,q.status FROM prerequisites p JOIN tasks q
      ON q.id=p.prerequisite_id WHERE p.task_id=? ORDER BY p.prerequisite_id''',
                      (key,)).fetchall()
    r = valid_record(json.loads(row['original_json']), 'task')
    r.update(revision=row['revision'], archived=bool(row['archived']), updated_utc=row['updated_utc'],
             blocked_by=[p['prerequisite_id'] for p in deps if p['status'] != 'completed'],
             ready=row['status'] == 'open' and not row['archived'] and all(p['status'] == 'completed' for p in deps))
    return r


def context_snapshot(db, ref, max_nodes=None):
    if not isinstance(ref, str) or ':' not in ref:
        raise ValueError('task/event source reference required')
    kind, key = ref.split(':', 1)
    if kind == 'note':
        import note_store
        return note_store.snapshot(db,ref)
    if kind not in ('task', 'event'):
        raise ValueError('task/event/note source reference required')
    identifier(key)
    if max_nodes is not None and (type(max_nodes) is not int or max_nodes < 1):
        raise ValueError('legacy max_nodes hint must be positive')
    # Compatibility hint never excludes necessary source evidence.
    with atomic(db), bounded_work(db):
        check_schema(db)
        dependents, records, evidence = [], {}, {}
        if kind == 'event':
            row = _row(db, kind, key)
            records[key] = valid_record(json.loads(row['original_json']), kind) | {
                'revision': row['revision'], 'archived': bool(row['archived'])}
            evidence[key] = (row['revision'], row['original_digest'])
        else:
            successors = db.execute('SELECT task_id FROM prerequisites WHERE prerequisite_id=? ORDER BY task_id',
                                    (key,)).fetchall()
            dependents = [r[0] for r in successors]
            pending = [key] + dependents
            while pending:
                check_work(db)
                current = pending.pop()
                if current in records:
                    continue
                row = _row(db, 'task', current)
                records[current] = _task(db, current)
                evidence[current] = (row['revision'], row['original_digest'])
                pending.extend(records[current]['prerequisites'])
        root = records[key]
        snapshot = {'namespace': meta(db, 'namespace'), 'ref': ref, 'kind': kind, 'root_id': key,
                    'records': [records[k] for k in sorted(records)], 'dependent_ids': dependents,
                    'sendable': root['status'] == ('active' if kind == 'event' else 'open') and not root['archived'],
                    'schema_version': SCHEMA_VERSION, 'backend': 'indexed_sqlite_v2',
                    'revision': root['revision'], 'data_only': True}
        # Complete original digests include private unknown extensions. Global
        # generation is deliberately absent: unrelated updates don't stale a due.
        snapshot['fingerprint'] = digest({'snapshot': snapshot, 'complete_source_digests': evidence})
        return bounded_result(snapshot)


def read_context(path, ref, max_nodes=None):
    db = connect(path, readonly=True)
    try:
        return context_snapshot(db, ref, max_nodes)
    finally:
        db.close()


def _cursor_encode(value):
    return base64.urlsafe_b64encode(compact_json(value).encode()).decode().rstrip('=')


def _cursor_decode(value):
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError('invalid cursor')
    try:
        result = json.loads(base64.urlsafe_b64decode(value + '=' * (-len(value) % 4)))
    except (ValueError, UnicodeError):
        raise ValueError('invalid cursor') from None
    if not isinstance(result, dict):
        raise ValueError('invalid cursor')
    return result


def list_records(db, data, search=False):
    allowed = {'kind', 'status', 'start', 'end', 'cursor', 'limit', 'archived', 'query'}
    if set(data) - allowed:
        raise ValueError('unsupported list field')
    kind = data.get('kind', 'tasks')
    if kind not in ('tasks', 'events'):
        raise ValueError('invalid list kind')
    limit = data.get('limit', 25)
    if type(limit) is not int or not 1 <= limit <= 25:
        raise ValueError('list limit must be 1..25')
    status = data.get('status')
    if status is not None and status not in (TASK_STATUSES if kind == 'tasks' else EVENT_STATUSES):
        raise ValueError('invalid status filter')
    archived = data.get('archived', False)
    if type(archived) is not bool:
        raise ValueError('explicit archive filter required')
    query = data.get('query')
    if search:
        if not isinstance(query, str) or not query.strip() or '\x00' in query:
            raise ValueError('bounded search text required')
    elif query is not None:
        raise ValueError('use search for lexical queries')
    field = 'updated_utc' if kind == 'tasks' else 'start_utc'
    filters = {k: data.get(k) for k in ('kind', 'status', 'start', 'end', 'archived', 'query')}
    filters['kind'], filters['archived'] = kind, archived
    ns, gen = meta(db, 'namespace'), int(meta(db, 'generation'))
    scope = digest(filters)
    where, params = ['t.archived=?'], [int(archived)]
    if status is not None:
        where.append('t.status=?'); params.append(status)
    for bound, op in (('start', '>='), ('end', '<')):
        v = data.get(bound)
        if v is not None:
            if not isinstance(v, str) or not v.endswith('Z') or 'T' not in v:
                raise ValueError('explicit UTC range required')
            try:
                v = datetime.fromisoformat(v[:-1] + '+00:00').isoformat(timespec='microseconds').replace('+00:00', 'Z')
            except ValueError:
                raise ValueError('invalid UTC range') from None
            where.append(f't.{field}{op}?'); params.append(v)
    if data.get('cursor') is not None:
        c = _cursor_decode(data['cursor'])
        if (set(c) != {'schema','namespace','generation','filter','time','id'}
                or type(c.get('schema')) is not int or type(c.get('generation')) is not int
                or c.get('schema') != SCHEMA_VERSION or c.get('namespace') != ns or c.get('generation') != gen
                or c.get('filter') != scope or not isinstance(c.get('time'), str)):
            raise ValueError('stale or mismatched cursor; refresh query')
        identifier(c.get('id'))
        where.append(f'(t.{field},t.id)>(?,?)'); params.extend([c['time'], c['id']])
    join = ''
    if search:
        # MATCH syntax cannot come from the caller; quote each literal token.
        terms = query.split()
        match = ' AND '.join('"' + term.replace('"', '""') + '"' for term in terms)
        join = ' JOIN source_fts f ON f.id=t.id AND f.kind=?'
        params = [kind[:-1]] + params
        where.append('source_fts MATCH ?'); params.append(match)
    q = f'SELECT t.id,t.title,t.status,t.revision,t.archived,t.{field} AS time FROM {kind} t{join} WHERE '
    q += ' AND '.join(where) + f' ORDER BY t.{field},t.id LIMIT ?'
    params.append(limit + 1)
    with bounded_work(db):
        rows = db.execute(q, params).fetchall()
    result = {'records': [], 'more': bool(rows), 'cursor': None, 'returned': 0,
              'namespace': ns, 'generation': gen, 'schema_version': SCHEMA_VERSION, 'data_only': True}
    for i, row in enumerate(rows[:limit]):
        check_work(db)
        record = {'id': row['id'], 'title_preview': row['title'][:160], 'preview': True,
                  'status': row['status'], 'revision': row['revision'], 'time': row['time']}
        c = _cursor_encode({'schema': SCHEMA_VERSION, 'namespace': ns, 'generation': gen,
                            'filter': scope, 'time': row['time'], 'id': row['id']})
        candidate = result | {'records': result['records'] + [record], 'returned': i + 1,
                              'cursor': c, 'more': i + 1 < len(rows)}
        if not fits(candidate):
            if not result['records']:
                raise ContextOverflow()
            result['more'] = True
            break
        result = candidate
    return bounded_result(result)


def _check_cycle(db, key):
    # Only paths reachable from the changed task; bounded iterative traversal.
    active, done, stack = set(), set(), [(key, False)]
    while stack:
        check_work(db)
        node, leaving = stack.pop()
        if leaving:
            active.remove(node); done.add(node); continue
        if node in active:
            raise ValueError('prerequisite cycle rejected')
        if node in done:
            continue
        active.add(node); stack.append((node, True))
        parents = db.execute('SELECT prerequisite_id FROM prerequisites WHERE task_id=?', (node,)).fetchall()
        stack.extend((r[0], False) for r in parents)


def run(db, action, data):
    if not isinstance(data, dict):
        raise ValueError('input must be an object')
    reads = ('init', 'metadata', 'context', 'task_get', 'event_get', 'list', 'search')
    with atomic(db, action not in reads), bounded_work(db):
        check_schema(db)
        if action in ('init', 'metadata'):
            if data:
                raise ValueError('metadata takes no fields')
            # Agent metadata avoids full export digest work; operator metadata()
            # above performs complete preservation verification explicitly.
            return {'schema_version': 2, 'namespace': meta(db, 'namespace'),
                    'generation': int(meta(db, 'generation')), 'data_only': True}
        if action in ('context', 'task_get', 'event_get'):
            if action == 'context':
                if set(data) - {'ref', 'max_nodes'}:
                    raise ValueError('unsupported context field')
                return context_snapshot(db, data['ref'], data.get('max_nodes'))
            if set(data) != {'id'}:
                raise ValueError('exact get takes only id')
            return context_snapshot(db, action.split('_')[0] + ':' + identifier(data['id']))
        if action in ('list', 'search'):
            return list_records(db, data, action == 'search')
        if action not in ('event_put', 'task_put', 'event_cancel', 'task_status', 'archive', 'restore'):
            raise ValueError('unknown source action')
        kind = data.get('kind') if action in ('archive', 'restore') else action.split('_')[0]
        if kind not in ('task', 'event'):
            raise ValueError('source kind required')
        key = identifier(data.get('id'))
        if db.execute("SELECT 1 FROM source_schedule_operations WHERE source_ref=? AND status='prepared' LIMIT 1",
                      (kind + ':' + key,)).fetchone():
            raise ValueError('pending schedule operation; reconcile before changing source')
        expected = data.get('expected_revision')
        if type(expected) is not int or expected < 0:
            raise ValueError('expected_revision required')
        old = db.execute(f'SELECT * FROM {kind}s WHERE id=?', (key,)).fetchone()
        if (old is None and expected != 0) or (old is not None and old['revision'] != expected):
            raise ValueError('source revision conflict')
        if old is None and action not in ('task_put', 'event_put'):
            raise ValueError('unknown source reference')
        original = json.loads(old['original_json']) if old is not None else None
        newly_ready = []
        if action in ('event_put', 'task_put'):
            if set(data) - {'id', 'expected_revision', 'record', 'changes'}:
                raise ValueError('unsupported write field')
            if ('record' in data) == ('changes' in data):
                raise ValueError('exactly one record or changes required')
            if 'changes' in data:
                if original is None or not isinstance(data['changes'], dict) or 'id' in data['changes']:
                    raise ValueError('existing record and non-ID changes required')
                original = original | data['changes']
            else:
                if old is not None:
                    raise ValueError('updates require changes to preserve private extensions')
                original = data['record']
            if not isinstance(original, dict) or original.get('id') != key:
                raise ValueError('record identity mismatch')
        elif action == 'event_cancel':
            if set(data) - {'id', 'expected_revision'}:
                raise ValueError('unsupported cancel field')
            original['status'] = 'cancelled'
        elif action == 'task_status':
            if set(data) - {'id', 'expected_revision', 'status', 'source'}:
                raise ValueError('unsupported status field')
            if data.get('status') not in TASK_STATUSES:
                raise ValueError('invalid task status')
            original['status'], original['source'] = data['status'], data['source']
            successors = db.execute('SELECT task_id FROM prerequisites WHERE prerequisite_id=? ORDER BY task_id',
                                    (key,)).fetchall()
            before = {r[0] for r in successors if _task(db, r[0])['ready']}
        archived = old['archived'] if old is not None else 0
        if action in ('archive', 'restore'):
            if set(data) - {'id', 'expected_revision', 'kind', 'references_clear'}:
                raise ValueError('unsupported archive field')
            if action == 'archive':
                if original['status'] not in ('completed', 'cancelled') or data.get('references_clear') is not True:
                    raise ValueError('terminal status and reviewed external reference clearance required')
                # Existing graph references are never moved out of recovery. The
                # D2 caller separately verifies live reminder/lineage references.
                if kind == 'task' and db.execute('SELECT 1 FROM prerequisites WHERE prerequisite_id=? LIMIT 1', (key,)).fetchone():
                    raise ValueError('referenced prerequisite cannot be archived')
                if db.execute("SELECT 1 FROM reminder_mappings WHERE source_ref=? AND status='active' LIMIT 1",
                              (kind + ':' + key,)).fetchone():
                    raise ValueError('live mapped reminder prevents archive')
            archived = int(action == 'archive')
        _store_record(db, kind, original, expected + 1,
                      old['ordinal'] if old is not None else db.execute(f'SELECT COALESCE(MAX(ordinal),-1)+1 FROM {kind}s').fetchone()[0],
                      archived)
        if kind == 'task':
            _check_cycle(db, key)
            if db.execute("SELECT 1 FROM metadata WHERE key='notes_schema'").fetchone():
                import note_store
                note_store.invalidate_rules(db,'task:'+key,action,
                    terminal=action!='task_put' or archived or original['status']!='open' or (old is not None and old['status']!='open'))
        if action == 'task_status':
            newly_ready = sorted({r[0] for r in successors if _task(db, r[0])['ready']} - before)
        generation = bump(db)
        ack = {'status': 'updated', 'ref': kind + ':' + key, 'revision': expected + 1,
               'generation': generation, 'namespace': meta(db, 'namespace'), 'data_only': True,
               'newly_ready': newly_ready, 'reminder_action_required': True}
        # If a long unlock-ID collection cannot fit, rollback rather than committing
        # an operation whose requested observable result cannot be represented.
        return bounded_result(ack)


def _schema_signature(db):
    return [(row['type'], row['name'], row['tbl_name'], ' '.join((row['sql'] or '').split()))
            for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]


def recognize_import_schema(db):
    """Read-only exact-schema recognition for the reviewed operator recovery gate."""
    check_schema(db)
    present = db.execute("SELECT 1 FROM sqlite_master WHERE name='historical_terminal_bindings'").fetchone() is not None
    template = sqlite3.connect(':memory:')
    template.row_factory = sqlite3.Row
    try:
        template.executescript(SCHEMA if present else SCHEMA.removesuffix(HISTORICAL_BINDINGS_SCHEMA))
        if _schema_signature(db) != _schema_signature(template):
            raise ValueError('recognized import schema required for binding extension')
        return {'historical_binding_table_present':present, 'data_only':True}
    finally:
        template.close()


def extend_import_schema(db, namespace, expected_export_digest):
    """Explicit operator-only extension of the exact recognized partial import.

    The caller must separately verify the reviewed previous helper/checkpoint,
    current original files/ledger, sole-writer pause and stopped helper. No normal
    source action invokes this function. No source records or revisions change.
    """
    canonical_namespace(namespace)
    with atomic(db, True):
        present = recognize_import_schema(db)['historical_binding_table_present']
        if (meta(db, 'namespace') != namespace or meta(db, 'generation') != '1'
                or meta(db, 'legacy_imported') != 'true'
                or digest(export_legacy(db)) != expected_export_digest
                or db.execute('SELECT 1 FROM reminder_mappings LIMIT 1').fetchone()
                or db.execute('SELECT 1 FROM source_schedule_operations LIMIT 1').fetchone()
                or (present and db.execute('SELECT 1 FROM historical_terminal_bindings LIMIT 1').fetchone())):
            raise ValueError('unchanged committed partial import required for binding extension')
        if not present:
            db.execute(HISTORICAL_BINDINGS_SCHEMA)
        return {'extended':not present, 'source_records_unchanged':True, 'data_only':True}


def historical_binding_reason(db, ref):
    """Read-only classification; unresolved refs never become actionable sources."""
    if not isinstance(ref, str) or not ref or '\x00' in ref:
        raise ValueError('unsupported existing ledger source binding')
    if ':' not in ref:
        return 'unsupported_reference'
    kind, key = ref.split(':', 1)
    if kind not in ('event', 'task') or not key.strip() or len(key) > 128:
        return 'unsupported_reference'
    return None if db.execute('SELECT 1 FROM '+('events' if kind == 'event' else 'tasks')+' WHERE id=?',(key,)).fetchone() else 'source_missing'


def import_ledger_bindings(db, rows):
    """Operator-only streaming cutover import from a consistent ledger snapshot.

    Does not change ledger rows or schedule anything. Caller must compare the
    paused live ledger snapshot; model-provided rows are not authorized input.
    """
    count = 0
    historical_count = 0
    with atomic(db, True):
        if meta(db, 'legacy_imported') != 'true' or meta(db, 'generation') != '1':
            raise ValueError('ledger binding import requires unchanged imported source')
        for supplied in rows:
            row = dict(supplied)
            ref = row.get('source_ref')
            if ref is None:
                continue
            rid = identifier(row.get('id'))
            rev, status = row.get('revision'), row.get('status')
            if (type(rev) is not int or rev < 1 or status not in
                    ('active', 'cancelled', 'expired', 'invalidated', 'transport_confirmed', 'dismissed')):
                raise ValueError('unsupported existing ledger row')
            reason = historical_binding_reason(db, ref)
            if reason is not None:
                if status not in TERMINAL_REMINDER_STATUSES:
                    raise ValueError('unresolved nonterminal ledger source binding')
                request = {'historical_terminal_binding':True, 'reminder_id':rid,
                           'reminder_revision':rev, 'source_ref':ref, 'status':status, 'reason':reason}
                raw = compact_json(request)
                receipt = 'historical-import:' + digest({'namespace':meta(db,'namespace'), 'reminder_id':rid})
                db.execute('INSERT INTO historical_terminal_bindings VALUES (?,?,?,?,?,?,?,?)',
                           (rid, ref, rev, status, reason, receipt, raw, digest(request)))
                historical_count += 1
                count += 1
                continue
            kind, key = ref.split(':', 1)
            source = _row(db, kind, key)
            request = {'imported_ledger_binding': True, 'reminder_id': rid,
                       'reminder_revision': rev, 'source_ref': ref, 'status': status}
            sha = digest(request)
            op = 'import:' + digest({'namespace': meta(db, 'namespace'), 'reminder_id': rid})
            db.execute('INSERT INTO source_schedule_operations VALUES (?,?,?,?,?,?,?,?)',
                       (op, sha, ref, source['revision'], source['original_digest'], compact_json(request), 'mapped', sha))
            db.execute('INSERT INTO reminder_mappings VALUES (?,?,?,?,?,?,?)',
                       (rid, ref, source['revision'], source['original_digest'], rev, status, op))
            count += 1
        if count:
            bump(db)
    return {'imported_bindings':count, 'source_mappings':count-historical_count,
            'historical_terminal_bindings':historical_count, 'data_only':True}


class SourceSchedule:
    """Private operator adapter for D2's durable journal; never a model tool.

    Source preparation freezes that exact record until mapping commits. Each
    boundary is idempotent, so restart can finish a receipt without a new timer.
    Failed prepared operations stay held for explicit operator reconciliation.
    """
    def __init__(self, db):
        self.db = db

    def _request(self, request, sha):
        if (not isinstance(request, dict) or set(request) !=
                {'operation_id', 'source_ref', 'expected_source_revision', 'action', 'args'}
                or digest(request) != sha):
            raise ValueError('invalid schedule request digest')
        op, ref = request['operation_id'], request['source_ref']
        if (not isinstance(op, str) or not 1 <= len(op) <= 128 or
                any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:' for c in op)):
            raise ValueError('invalid schedule operation identifier')
        if not isinstance(ref, str) or ':' not in ref:
            raise ValueError('source reference required')
        kind, key = ref.split(':', 1)
        if kind not in ('task', 'event', 'note'):
            raise ValueError('source reference required')
        identifier(key)
        rev, action, args = request['expected_source_revision'], request['action'], request['args']
        if type(rev) is not int or rev < 1 or action not in ('create', 'update', 'cancel', 'snooze', 'create_followup') or not isinstance(args, dict):
            raise ValueError('invalid schedule operation')
        if action == 'create' and args.get('source_ref') != ref:
            raise ValueError('create must bind exact source')
        if 'source_ref' in args and args['source_ref'] != ref:
            raise ValueError('source rebinding requires a new explicit operation')
        if kind=='note' and action=='create' and args.get('mode')!='contextual':
            raise ValueError('note returns require contextual freshness checks')
        return op, ref, kind, key

    def prepare_schedule(self, request, sha):
        op, ref, kind, key = self._request(request, sha)
        with atomic(self.db, True), bounded_work(self.db):
            prior = self.db.execute('SELECT * FROM source_schedule_operations WHERE operation_id=?', (op,)).fetchone()
            if prior is not None:
                if prior['digest'] != sha:
                    raise ValueError('conflicting schedule operation')
                return  # Already frozen or durably mapped; exact request only.
            if self.db.execute("SELECT 1 FROM source_schedule_operations WHERE source_ref=? AND status='prepared' LIMIT 1", (ref,)).fetchone():
                raise ValueError('source already has pending schedule operation')
            row = _row(self.db, kind, key)
            if row['revision'] != request['expected_source_revision']:
                raise ValueError('source revision conflict')
            if request['action'] != 'cancel' and (row['archived'] or row['status'] != ('active' if kind in ('event','note') else 'open')):
                raise ValueError('source is no longer schedulable')
            args = request['args']
            if request['action']=='create' and self.db.execute("SELECT 1 FROM metadata WHERE key='notes_schema'").fetchone():
                plan=self.db.execute('SELECT * FROM return_decisions WHERE source_ref=?',(ref,)).fetchone()
                explicit=args.get('owner_request_wire_id')
                if explicit is not None:
                    if not isinstance(explicit,str) or not 1<=len(explicit.encode())<=128:
                        raise ValueError('exact genuine Owner request provenance required')
                delegated_task=(plan is not None and plan['decision'] not in ('no_return','unresolved_context')
                                and args.get('mode')=='contextual' and args.get('followup_limit',0)==0 and explicit is None)
                if kind=='note' or delegated_task:
                    if (plan is None or plan['decision']!='planned_timed' or plan['mechanism_id']!=op
                            or plan['source_revision']!=row['revision'] or args.get('mode')!='contextual'
                            or args.get('followup_limit',0)!=0):
                        raise ValueError('return mechanism already selected or exact timed plan missing')
                    import reminder_tools as runtime
                    from datetime import datetime
                    due=runtime.local_instant(args['local_time'],args['timezone'],args.get('fold'))
                    late=args.get('late_window_seconds',3600)
                    deadline=datetime.fromisoformat(plan['deadline'][:-1]+'+00:00').timestamp()
                    if type(late) not in (int,float) or not 1<=late<=3600 or due+late>deadline:
                        raise ValueError('timer exceeds planned relevance before preparation')
            if request['action'] != 'create':
                if request['action'] == 'create_followup':
                    identifier(args.get('parent_event_id'))
                    parent = identifier(args.get('parent_reminder_id'))
                else:
                    parent = args.get('id')
                bound = self.db.execute('SELECT * FROM reminder_mappings WHERE reminder_id=?', (parent,)).fetchone()
                if (bound is None or bound['source_ref'] != ref or type(args.get('expected_revision')) is not int
                        or bound['reminder_revision'] != args['expected_revision']):
                    raise ValueError('schedule mapping missing or stale')
            self.db.execute('INSERT INTO source_schedule_operations VALUES (?,?,?,?,?,?,?,NULL)',
                            (op, sha, ref, row['revision'], row['original_digest'], compact_json(request), 'prepared'))

    def map_schedule(self, request, sha, result):
        op, ref, kind, key = self._request(request, sha)
        if (not isinstance(result, dict)
                or type(result.get('revision')) is not int or result['revision'] < 1
                or result.get('source_ref') != ref or result.get('status') not in
                ('active', 'cancelled', 'expired', 'invalidated', 'transport_confirmed', 'dismissed')):
            raise ValueError('invalid ledger mapping receipt')
        rid = identifier(result.get('id'))
        child = request['action'] in ('create','create_followup')
        if not child and rid != request['args'].get('id'):
            raise ValueError('ledger receipt identity mismatch')
        if not child and result['revision'] != request['args']['expected_revision'] + 1:
            raise ValueError('ledger receipt revision mismatch')
        if child and result['revision'] != 1:
            raise ValueError('create receipt revision mismatch')
        result_sha = digest(result)
        with atomic(self.db, True), bounded_work(self.db):
            prior = self.db.execute('SELECT * FROM source_schedule_operations WHERE operation_id=?', (op,)).fetchone()
            if prior is None or prior['digest'] != sha:
                raise ValueError('schedule preparation missing or changed')
            if prior['status'] == 'mapped':
                if prior['result_digest'] != result_sha:
                    raise ValueError('conflicting mapped receipt')
                return
            row = _row(self.db, kind, key)
            if row['revision'] != prior['source_revision'] or row['original_digest'] != prior['source_digest']:
                raise ValueError('prepared source changed')
            existing = self.db.execute('SELECT * FROM reminder_mappings WHERE reminder_id=?', (rid,)).fetchone()
            if child and existing is not None:
                raise ValueError('create receipt already mapped')
            self.db.execute('''INSERT INTO reminder_mappings VALUES (?,?,?,?,?,?,?)
              ON CONFLICT(reminder_id) DO UPDATE SET source_ref=excluded.source_ref,
              source_revision=excluded.source_revision,source_digest=excluded.source_digest,
              reminder_revision=excluded.reminder_revision,status=excluded.status,operation_id=excluded.operation_id''',
                            (rid, ref, row['revision'], row['original_digest'], result['revision'], result['status'], op))
            self.db.execute("UPDATE source_schedule_operations SET status='mapped',result_digest=? WHERE operation_id=?", (result_sha, op))
            bump(self.db)


def export_state(db):
    """Operator-only complete semantic export, including internal receipts.

    export_legacy() preserves original source records/opaque partitions only.
    Rollback after new writes must preserve this entire state or SQLite backup,
    and cannot resume an old writer that cannot represent schema2 tasks.
    """
    with atomic(db):
        tables=('metadata','legacy_partitions','events','tasks','source_revisions','fts_documents','prerequisites',
                'curated_notes','semantic_pointers','source_schedule_operations','reminder_mappings','historical_terminal_bindings')
        if db.execute("SELECT 1 FROM metadata WHERE key='notes_schema'").fetchone():
            import note_store
            note_store.check(db)
            tables+=note_store.TABLES
        # BLOBs use explicit hex for safe lossless operator-only canonical export.
        return {'schema_version': SCHEMA_VERSION,'tables':{
          table:[{k:({'blob_hex':v.hex()} if isinstance(v,bytes) else v) for k,v in dict(r).items()}
                 for r in db.execute('SELECT * FROM '+table)] for table in tables}}


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--db', required=True)
    p.add_argument('action', choices=('init', 'metadata', 'context', 'task_get', 'event_get', 'list', 'search',
                                    'event_put', 'task_put', 'event_cancel', 'task_status', 'restore'))
    opts = p.parse_args()
    db = None
    try:
        raw = sys.stdin.buffer.read()
        import legacy_source
        data = json.loads(raw, object_pairs_hook=legacy_source.pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON rejected'))) if raw.strip() else {}
        if not isinstance(data, dict):
            raise ValueError('input must be an object')
        db = connect(Path(opts.db), initialize=opts.action == 'init', namespace=data.get('namespace') if opts.action == 'init' else None)
        with atomic(db, opts.action not in ('init', 'metadata', 'context', 'task_get', 'event_get', 'list', 'search')):
            value = run(db, opts.action, {} if opts.action == 'init' else data)
            # Refuse before mutation commit if even the final wrapper cannot fit.
            output = compact_json(bounded_result({'ok': True, 'result': value}, 'source'))
        print(output)
        return 0
    except (ValueError, KeyError, TypeError, sqlite3.Error, OSError, UnicodeError):
        print(compact_json({'ok': False, 'status': 'source_hold', 'data_only': True, 'sendable': False}))
        return 1
    finally:
        if db is not None:
            db.close()


if __name__ == '__main__':
    sys.exit(main())
