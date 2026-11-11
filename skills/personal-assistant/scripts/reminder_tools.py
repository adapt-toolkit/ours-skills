#!/usr/bin/env python3
"""Private durable due-only reminders. JSON stdin tools; no periodic model calls."""
import argparse
import base64
import fcntl
import hashlib
import hmac
import importlib.util
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

SCHEMA = '''
CREATE TABLE IF NOT EXISTS reminders (
 id TEXT PRIMARY KEY, revision INTEGER NOT NULL, title TEXT NOT NULL,
 context TEXT NOT NULL, source_ref TEXT, local_time TEXT NOT NULL,
 timezone TEXT NOT NULL, fold INTEGER, due REAL NOT NULL, expires REAL NOT NULL,
 status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (
 event_id TEXT PRIMARY KEY, reminder_id TEXT NOT NULL, revision INTEGER NOT NULL,
 status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0,
 prompt_id TEXT, queued_behind INTEGER, delivery_status TEXT, wire_id TEXT,
 UNIQUE(reminder_id, revision));
CREATE TABLE IF NOT EXISTS followups (
 parent_event_id TEXT PRIMARY KEY, child_id TEXT NOT NULL UNIQUE,
 request_fingerprint TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS operation_receipts (
 operation_id TEXT PRIMARY KEY, action TEXT NOT NULL,
 request_sha256 TEXT NOT NULL, response_json TEXT NOT NULL,
 linked INTEGER NOT NULL DEFAULT 0 CHECK(linked IN (0,1)));
CREATE TABLE IF NOT EXISTS reminder_operation_holds (
 reminder_id TEXT PRIMARY KEY REFERENCES reminders(id), operation_id TEXT NOT NULL UNIQUE
 REFERENCES operation_receipts(operation_id));
CREATE TABLE IF NOT EXISTS ledger_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS recovery_tickets (
 event_id TEXT PRIMARY KEY REFERENCES events(event_id),prompt_id TEXT NOT NULL,
 state_json TEXT NOT NULL,stopped INTEGER NOT NULL DEFAULT 0 CHECK(stopped IN(0,1)));
CREATE INDEX IF NOT EXISTS reminders_status_due_id ON reminders(status,due,id);
CREATE INDEX IF NOT EXISTS reminders_status_expires_id ON reminders(status,expires,id);
CREATE INDEX IF NOT EXISTS reminders_due_id ON reminders(due,id);
CREATE INDEX IF NOT EXISTS events_status_retry_id ON events(status,retry_at,event_id);
CREATE INDEX IF NOT EXISTS events_reminder_revision ON events(reminder_id,revision);
CREATE TABLE IF NOT EXISTS schedule_queue (
 reminder_id TEXT PRIMARY KEY REFERENCES reminders(id),revision INTEGER NOT NULL,due REAL NOT NULL);
CREATE INDEX IF NOT EXISTS schedule_queue_due_id ON schedule_queue(due,reminder_id);
'''

BATCH_SIZE = 25

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)

def unique_pairs(items):
    result={}
    for key,value in items:
        if key in result:raise ValueError('duplicate JSON field')
        result[key]=value
    return result

def strict_json(raw):
    return json.loads(raw,object_pairs_hook=unique_pairs,
        parse_constant=lambda _:(_ for _ in ()).throw(ValueError('nonfinite JSON rejected')))

def local_instant(local_time, zone, fold=None):
    """Reject DST gaps and require explicit fold for repeated wall time."""
    if not isinstance(local_time, str) or 'T' not in local_time or len(local_time) < 16:
        raise ValueError('explicit local date and time required')
    wall = datetime.fromisoformat(local_time)
    if wall.tzinfo is not None:
        raise ValueError('local_time must be an explicit local ISO date/time without offset')
    tz = ZoneInfo(zone)
    candidates = {}
    for value in (0, 1):
        aware = wall.replace(tzinfo=tz, fold=value)
        utc = aware.astimezone(timezone.utc)
        if utc.astimezone(tz).replace(tzinfo=None) == wall:
            candidates[value] = utc.timestamp()
    if not candidates:
        raise ValueError('nonexistent local time during DST transition')
    if len(set(candidates.values())) > 1 and fold is None:
        raise ValueError('ambiguous local time: specify fold 0 or 1 after user confirmation')
    if fold is not None and (type(fold) is not int or fold not in (0, 1)):
        raise ValueError('fold must be 0 or 1')
    return candidates[0 if fold is None else fold]

class Store:
    def __init__(self, path):
        path = Path(path)
        if not path.is_absolute():raise ValueError('absolute private ledger path required')
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        info=path.parent.stat()
        if path.parent.is_symlink() or info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=0o700:
            raise ValueError('private ledger directory required')
        try:
            fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600);os.close(fd)
        except FileExistsError:pass
        info=path.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_nlink!=1 or stat.S_IMODE(info.st_mode)!=0o600:
            raise ValueError('private owner mode600 regular ledger required')
        if info.st_size:
            check=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
            try:
                for table,fields in (('reminders',('id','revision','title','context','source_ref','local_time','timezone','fold','due','expires','status')),
                                     ('events',('event_id','reminder_id','revision','status','attempts','retry_at','prompt_id','queued_behind','delivery_status','wire_id'))):
                    actual=tuple(row[1] for row in check.execute('PRAGMA table_info('+table+')'))
                    if actual[:len(fields)]!=fields:raise ValueError('unexpected ledger schema; reviewed operator migration required')
            finally:check.close()
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript(SCHEMA)
        # Additive migration; legacy reminders stay fixed with no follow-up permission.
        def migrate():
            additions = {
                'reminders': [('mode', "TEXT NOT NULL DEFAULT 'fixed'"),
                    ('followup_limit', 'INTEGER NOT NULL DEFAULT 0'),
                    ('followup_deadline', 'REAL'), ('followup_min_seconds', 'INTEGER NOT NULL DEFAULT 3600')],
                'events': [('body_sha256', 'TEXT'), ('source_fingerprint', 'TEXT'), ('decision_reason', 'TEXT')],
            }
            for table, fields in additions.items():
                names = {r['name'] for r in self.db.execute('PRAGMA table_info(' + table + ')')}
                for name, definition in fields:
                    if name not in names:
                        self.db.execute('ALTER TABLE ' + table + ' ADD COLUMN ' + name + ' ' + definition)
        self.transaction(migrate)
        def initialize_metadata():
            for key,value in (('namespace',str(uuid.uuid4())),('generation','0'),('cursor_key',os.urandom(32).hex()),('contextual_enabled','true')):
                self.db.execute('INSERT OR IGNORE INTO ledger_metadata VALUES (?,?)',(key,value))
            # Derived dispatch index keeps disabled contextual retries out of the
            # scheduler's bounded fixed-only reads without changing event claims.
            if 'dispatch_mode' not in {r['name'] for r in self.db.execute('PRAGMA table_info(events)')}:
                self.db.execute("ALTER TABLE events ADD COLUMN dispatch_mode TEXT NOT NULL DEFAULT 'fixed'")
                self.db.execute('UPDATE events SET dispatch_mode=(SELECT mode FROM reminders WHERE id=events.reminder_id)')
            self.db.execute('CREATE INDEX IF NOT EXISTS events_dispatch_retry_id ON events(status,dispatch_mode,retry_at,event_id)')
            # Persistent generation binds cursors across processes and commits.
            # Receipts are append-only; every reminder/event/lineage mutation
            # invalidates an outstanding page rather than silently skipping rows.
            for table in ('reminders','events','followups'):
                for verb in ('INSERT','UPDATE','DELETE'):
                    self.db.execute(f'''CREATE TRIGGER IF NOT EXISTS ledger_{table}_{verb.lower()}
                        AFTER {verb} ON {table} BEGIN
                        UPDATE ledger_metadata SET value=CAST(value AS INTEGER)+1 WHERE key='generation'; END''')
            # Queue is derived state, never a replacement for immutable events.
            # Seed once per candidate schema, then maintain in each mutation txn.
            if self.db.execute("SELECT 1 FROM ledger_metadata WHERE key='queue_initialized'").fetchone() is None:
                self.db.execute('''INSERT OR IGNORE INTO schedule_queue SELECT r.id,r.revision,r.due FROM reminders r
                    WHERE r.status='active' AND NOT EXISTS(SELECT 1 FROM events e WHERE e.reminder_id=r.id AND e.revision=r.revision)
                    AND NOT EXISTS(SELECT 1 FROM reminder_operation_holds h WHERE h.reminder_id=r.id)''')
                self.db.execute("INSERT INTO ledger_metadata VALUES ('queue_initialized','1')")
            for verb in ('INSERT','UPDATE'):
                self.db.execute(f'''CREATE TRIGGER IF NOT EXISTS queue_reminders_{verb.lower()} AFTER {verb} ON reminders BEGIN
                    DELETE FROM schedule_queue WHERE reminder_id=NEW.id;
                    INSERT OR IGNORE INTO schedule_queue SELECT NEW.id,NEW.revision,NEW.due
                      WHERE NEW.status='active' AND NOT EXISTS(SELECT 1 FROM events WHERE reminder_id=NEW.id AND revision=NEW.revision)
                      AND NOT EXISTS(SELECT 1 FROM reminder_operation_holds WHERE reminder_id=NEW.id)
                      AND (NEW.mode='fixed' OR (SELECT value FROM ledger_metadata WHERE key='contextual_enabled')='true');
                    UPDATE events SET status='invalidated' WHERE reminder_id=NEW.id AND status='pending'
                      AND (revision!=NEW.revision OR NEW.status!='active'); END''')
            self.db.execute('''CREATE TRIGGER IF NOT EXISTS queue_events_insert AFTER INSERT ON events BEGIN
                DELETE FROM schedule_queue WHERE reminder_id=NEW.reminder_id AND revision=NEW.revision;
                UPDATE events SET dispatch_mode=(SELECT mode FROM reminders WHERE id=NEW.reminder_id) WHERE event_id=NEW.event_id; END''')
            self.db.execute('''CREATE TRIGGER IF NOT EXISTS queue_holds_insert AFTER INSERT ON reminder_operation_holds BEGIN
                DELETE FROM schedule_queue WHERE reminder_id=NEW.reminder_id;
                UPDATE ledger_metadata SET value=CAST(value AS INTEGER)+1 WHERE key='generation'; END''')
            self.db.execute('''CREATE TRIGGER IF NOT EXISTS queue_holds_delete AFTER DELETE ON reminder_operation_holds BEGIN
                INSERT OR IGNORE INTO schedule_queue SELECT r.id,r.revision,r.due FROM reminders r
                  WHERE r.id=OLD.reminder_id AND r.status='active'
                  AND (r.mode='fixed' OR (SELECT value FROM ledger_metadata WHERE key='contextual_enabled')='true')
                  AND NOT EXISTS(SELECT 1 FROM events e WHERE e.reminder_id=r.id AND e.revision=r.revision);
                UPDATE ledger_metadata SET value=CAST(value AS INTEGER)+1 WHERE key='generation'; END''')
        self.transaction(initialize_metadata)

    def metadata(self,key):
        return self.db.execute('SELECT value FROM ledger_metadata WHERE key=?',(key,)).fetchone()[0]

    def contextual_enabled(self):
        return self.metadata('contextual_enabled')=='true'

    def configure_policy(self,enabled):
        if type(enabled) is not bool:raise ValueError('contextual_enabled must be boolean')
        def change():
            value='true' if enabled else 'false'
            if self.metadata('contextual_enabled')==value:return
            self.db.execute("UPDATE ledger_metadata SET value=? WHERE key='contextual_enabled'",(value,))
            # Administrative policy transition only. Claims, source mappings,
            # reminders and receipts remain untouched; queue is derived state.
            self.db.execute('DELETE FROM schedule_queue')
            self.db.execute('''INSERT INTO schedule_queue SELECT r.id,r.revision,r.due FROM reminders r
                WHERE r.status='active' AND (r.mode='fixed' OR ?)
                AND NOT EXISTS(SELECT 1 FROM events e WHERE e.reminder_id=r.id AND e.revision=r.revision)
                AND NOT EXISTS(SELECT 1 FROM reminder_operation_holds h WHERE h.reminder_id=r.id)''',(int(enabled),))
        self.transaction(change)

    def recovery_budget(self,event_id,budget_module):
        event=self.event(event_id)
        prompt=event['prompt_id']
        if not isinstance(prompt,str) or not prompt.strip() or len(prompt)>128:
            raise ValueError('trusted actual admission prompt receipt required for recovery budget')
        ticket=self.db.execute('SELECT * FROM recovery_tickets WHERE event_id=?',(event_id,)).fetchone()
        if ticket is not None:
            if ticket['stopped'] or ticket['prompt_id']!=prompt:return None
            return budget_module.TurnBudget(json.loads(ticket['state_json']))
        return budget_module.TurnBudget()

    def save_recovery_budget(self,event_id,budget,stopped=False):
        prompt=self.event(event_id)['prompt_id']
        self.db.execute('''INSERT INTO recovery_tickets VALUES (?,?,?,?) ON CONFLICT(event_id)
            DO UPDATE SET state_json=excluded.state_json,stopped=excluded.stopped''',
            (event_id,prompt,canonical(budget.snapshot()),int(stopped)))

    def inspect_budgeted(self,event_id,budget_module,reader=None,now=None):
        """One ledger-owned due ticket; repeated processes cannot reset its charges."""
        def inspect():
            budget=self.recovery_budget(event_id,budget_module)
            if budget is None:return None  # Exhausted ticket emits no further context.
            view=self.tool('inspect',{'event_id':event_id},now)
            e,r=view['event'],view['reminder']
            result={'event':{k:e[k] for k in ('event_id','revision','status','delivery_status')},
                    'reminder':{k:r[k] for k in ('id','revision','mode','due','expires','status','timezone')},
                    'eligible':view['eligible'],'data_only':True}
            if r['mode']=='contextual':
                result['reminder'].update({k:r[k] for k in ('title','context','source_ref')})
                if r['followup_limit']:
                    result['reminder'].update({k:r[k] for k in ('followup_limit','followup_deadline','followup_min_seconds')})
            try:
                if not view['eligible'] or e['status'] not in ('queued','uncertain','submitting') or e['delivery_status'] is not None:
                    result=budget_module.failure('invalid_request')
                elif r['mode']=='contextual':
                    try:
                        snapshot=self.context_snapshot(r,reader)
                        fingerprint=snapshot.get('fingerprint') if isinstance(snapshot,dict) else None
                        if (not isinstance(fingerprint,str) or len(fingerprint)!=64 or
                                any(c not in '0123456789abcdef' for c in fingerprint)):
                            raise ValueError('invalid source freshness guard')
                        result['recovered_context']=snapshot
                    except (ValueError,OSError,sqlite3.Error):
                        result=budget_module.failure('source_unavailable')
                budget.admit(result,source_keys=('recovered_context',),
                             source_fields=(('reminder','title'),('reminder','context')),
                             control_fields=(('recovered_context','fingerprint'),))
                self.save_recovery_budget(event_id,budget)
                return result
            except budget_module.ContextOverflow as error:
                try:result=budget.admit_failure(str(error))
                except budget_module.ContextOverflow:
                    self.save_recovery_budget(event_id,budget,True);return None
                self.save_recovery_budget(event_id,budget,True)
                return result
        return self.transaction(inspect)

    def reserve_action_response(self,event_id,budget_module):
        """Reserve the exact worst-case summary bound BEFORE send or mutation.

        This is conservative accounting, never a local size admission gate.
        Actual action output is additionally charged after completion.
        """
        def reserve():
            budget=self.recovery_budget(event_id,budget_module)
            if budget is None:return False
            bound=self.action_response_bound(event_id)
            row=budget.state['slots']['control'];total=budget.state['total']
            for field in ('tokens','bytes'):
                row[field]+=bound;total[field]+=bound
            row['calls']+=1;self.save_recovery_budget(event_id,budget)
            return True
        return self.transaction(reserve)

    def action_response_bound(self,event_id):
        # Every possible public action result has only these allowlisted fields.
        # New child IDs are UUIDs and revision1; admitted legacy event IDs are
        # measured before a claim. Long IDs therefore HOLD before external work.
        event={'event_id':self.event(event_id)['event_id'],'status':'transport_confirmed','delivery_status':'transport_confirmed'}
        child={'id':'0'*36,'revision':1,'status':'transport_confirmed','scheduled':True}
        return max(128,len(canonical(event).encode()),len(canonical(child).encode()))

    def receipt(self,operation_id):
        row=self.db.execute('SELECT action,request_sha256,response_json,linked FROM operation_receipts WHERE operation_id=?',
                            (operation_id,)).fetchone()
        return None if row is None else {'action':row['action'],'request_sha256':row['request_sha256'],
                                        'result':json.loads(row['response_json']),'linked':bool(row['linked'])}

    def complete_operation(self,operation_id):
        """Journal calls only after its source mapping ACK committed; no send."""
        def complete():
            receipt=self.receipt(operation_id)
            if receipt is None:raise ValueError('unknown operation receipt')
            result=receipt['result']
            if receipt['linked']:return {'status':'linked','operation_id':operation_id}
            current=self.reminder(result['id'])
            if current['revision']!=result['revision']:
                raise ValueError('operation reminder changed; reconcile before releasing hold')
            hold=self.db.execute('SELECT operation_id FROM reminder_operation_holds WHERE reminder_id=?',(result['id'],)).fetchone()
            if hold is None or hold[0]!=operation_id:raise ValueError('operation hold mismatch')
            self.db.execute('UPDATE operation_receipts SET linked=1 WHERE operation_id=?',(operation_id,))
            self.db.execute('DELETE FROM reminder_operation_holds WHERE reminder_id=?',(result['id'],))
            return {'status':'linked','operation_id':operation_id}
        return self.transaction(complete)

    def operation(self,operation_id,action,args,mutation):
        if operation_id is None:
            return mutation()
        if (not isinstance(operation_id,str) or not 1<=len(operation_id)<=128
                or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.' for c in operation_id)):
            raise ValueError('stable operation_id must be 1..128 ASCII identifier characters')
        digest=hashlib.sha256(canonical({'action':action,'args':args}).encode()).hexdigest()
        old=self.receipt(operation_id)
        if old is not None:
            if old['action']!=action or old['request_sha256']!=digest:
                raise ValueError('operation_id reused with a conflicting request; reconcile before retry')
            return old['result']
        result=mutation()
        self.db.execute('INSERT INTO operation_receipts(operation_id,action,request_sha256,response_json) VALUES (?,?,?,?)',
                        (operation_id,action,digest,canonical(result)))
        self.db.execute('INSERT INTO reminder_operation_holds VALUES (?,?)',(result['id'],operation_id))
        return result

    def list_page(self,args,budget_module=None):
        allowed={'status','due_from','due_to','limit','cursor'}
        if set(args)-allowed:
            raise ValueError('list takes only status/time/limit/cursor filters')
        status=args.get('status','active')
        statuses=('active','cancelled','expired','invalidated','transport_confirmed','dismissed','handled_internal')
        if status is not None and status not in statuses:
            raise ValueError('unsupported reminder status filter')
        limit=args.get('limit',BATCH_SIZE)
        if type(limit) is not int or not 1<=limit<=BATCH_SIZE:
            raise ValueError('page limit must be 1..25')
        filters={'status':status,'due_from':args.get('due_from'),'due_to':args.get('due_to')}
        for name in ('due_from','due_to'):
            v=filters[name]
            if v is not None and (type(v) not in (int,float) or not math.isfinite(v)):
                raise ValueError('due range requires finite UTC timestamps')
        if filters['due_from'] is not None and filters['due_to'] is not None and filters['due_from']>filters['due_to']:
            raise ValueError('due range is reversed')
        generation=int(self.metadata('generation'))
        binding={'namespace':self.metadata('namespace'),'generation':generation,'filters':filters,'sort':'due,id'}
        key=bytes.fromhex(self.metadata('cursor_key'))
        last=None
        cursor=args.get('cursor')
        if cursor is not None:
            try:
                if not isinstance(cursor,str) or len(cursor)>2048:raise ValueError()
                payload,signature=cursor.split('.')
                encoded=payload.encode('ascii')
                if not hmac.compare_digest(signature,hmac.new(key,encoded,hashlib.sha256).hexdigest()):raise ValueError()
                decoded=json.loads(base64.urlsafe_b64decode(encoded+b'='*(-len(encoded)%4)))
                if set(decoded)!=set(binding)|{'last'} or any(decoded[k]!=v for k,v in binding.items()):raise ValueError()
                last=decoded['last']
                if (not isinstance(last,list) or len(last)!=2 or type(last[0]) not in (int,float)
                        or not math.isfinite(last[0]) or not isinstance(last[1],str)):raise ValueError()
            except (ValueError,TypeError,UnicodeError,KeyError):
                raise ValueError('cursor invalid, changed filters or stale snapshot; refresh page') from None
        clauses=[];params=[]
        if status is not None:clauses.append('status=?');params.append(status)
        if filters['due_from'] is not None:clauses.append('due>=?');params.append(filters['due_from'])
        if filters['due_to'] is not None:clauses.append('due<=?');params.append(filters['due_to'])
        if last is not None:clauses.append('(due,id)>(?,?)');params.extend(last)
        where=' WHERE '+' AND '.join(clauses) if clauses else ''
        rows=self.db.execute('SELECT id,revision,title,source_ref,due,expires,status,mode FROM reminders'+where+
                             ' ORDER BY due,id LIMIT ?',(*params,limit+1)).fetchall()
        output=[dict(r) for r in rows[:limit]]
        for r in output:
            r['schedule_link_pending']=bool(self.db.execute('SELECT 1 FROM reminder_operation_holds WHERE reminder_id=?',(r['id'],)).fetchone())
        def envelope():
            next_cursor=None
            more=len(rows)>len(output)
            if more and output:
                data=canonical({**binding,'last':[output[-1]['due'],output[-1]['id']]}).encode()
                payload=base64.urlsafe_b64encode(data).rstrip(b'=')
                next_cursor=payload.decode()+'.'+hmac.new(key,payload,hashlib.sha256).hexdigest()
            return {'items':output,'returned':len(output),'more':more,'cursor':next_cursor,'data_only':True}
        result=envelope()
        return result

    def public_list(self,args,budget_module):
        self.db.execute('BEGIN')
        try:return self.list_page(args,budget_module)
        finally:self.db.execute('ROLLBACK')

    def transaction(self, fn):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            result = fn()
            self.db.execute('COMMIT')
            return result
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def reminder(self, ident):
        r = self.db.execute('SELECT * FROM reminders WHERE id=?', (ident,)).fetchone()
        if r is None:
            raise ValueError('unknown reminder')
        return dict(r)

    def event(self, ident):
        r = self.db.execute('SELECT * FROM events WHERE event_id=?', (ident,)).fetchone()
        if r is None:
            raise ValueError('unknown event')
        return dict(r)

    def tool(self, action, args, now=None, context_reader=None, operation_id=None,validate_only=False):
        now = time.time() if now is None else now
        allowed = {
            'create': {'title', 'context', 'source_ref', 'local_time', 'timezone', 'fold', 'late_window_seconds',
                       'mode', 'followup_limit', 'followup_until_utc', 'followup_min_seconds', 'owner_request_wire_id'},
            'update': {'id', 'expected_revision', 'title', 'context', 'source_ref', 'local_time', 'timezone', 'fold', 'late_window_seconds',
                       'mode', 'followup_limit', 'followup_until_utc', 'followup_min_seconds'},
            'snooze': {'id', 'expected_revision', 'local_time', 'timezone', 'fold', 'late_window_seconds'},
            'cancel': {'id', 'expected_revision'}, 'list': {'status','due_from','due_to','limit','cursor'}, 'inspect': {'event_id'},
        }
        if not isinstance(args, dict) or action not in allowed or set(args) - allowed[action]:
            raise ValueError('unknown action or arguments; recipients/routes cannot be supplied by callers')
        if 'owner_request_wire_id' in args:
            wire=args['owner_request_wire_id']
            if not isinstance(wire,str) or not 1<=len(wire.encode())<=128:
                raise ValueError('exact genuine Owner request provenance required')
        if action == 'list':
            if operation_id is not None:raise ValueError('operation_id is only for explicit reminder mutations')
            self.db.execute('BEGIN')
            try:return self.list_page(args)
            finally:self.db.execute('ROLLBACK')
        if action == 'inspect':
            if operation_id is not None:raise ValueError('operation_id is only for explicit reminder mutations')
            e = self.event(args['event_id'])
            r = self.reminder(e['reminder_id'])
            linked = not self.db.execute('SELECT 1 FROM reminder_operation_holds WHERE reminder_id=?',(r['id'],)).fetchone()
            valid = linked and r['revision'] == e['revision'] and r['status'] == 'active' and r['due'] <= now < r['expires'] and self.lineage_valid(r['id'])
            valid = valid and (r['mode']=='fixed' or self.contextual_enabled())
            return {'event': e, 'reminder': r, 'eligible': valid,
                    'stored_context_fingerprint': stored_fingerprint(r) if r['mode'] == 'contextual' and not r['source_ref'] else None}
        def mutate():
            old = None if action == 'create' else self.reminder(args['id'])
            if old and self.db.execute('SELECT 1 FROM reminder_operation_holds WHERE reminder_id=?',(old['id'],)).fetchone():
                raise ValueError('source schedule operation pending; reconcile before changing reminder')
            if old and (type(args.get('expected_revision')) is not int or args['expected_revision'] != old['revision']):
                raise ValueError('stale reminder revision')
            if old:
                busy = self.db.execute('''WITH RECURSIVE descendants(id) AS (
                    SELECT ? UNION SELECT f.child_id FROM descendants d
                    JOIN events e ON e.reminder_id=d.id
                    JOIN followups f ON f.parent_event_id=e.event_id)
                    SELECT 1 FROM events e JOIN descendants d ON e.reminder_id=d.id
                    WHERE e.delivery_status IN ('sending','uncertain') LIMIT 1''', (old['id'],)).fetchone()
                if busy:
                    raise ValueError('uncertain delivery: reconcile this reminder or descendant before changing it')
                if action != 'cancel' and old['status'] in ('transport_confirmed', 'dismissed', 'handled_internal'):
                    raise ValueError('terminal reminder cannot be reopened; use the event-bound follow-up or a new intake')
            if action == 'cancel':
                self.db.execute("UPDATE reminders SET status='cancelled', revision=revision+1 WHERE id=?", (old['id'],))
                return self.reminder(old['id'])
            if action == 'snooze' and not all(k in args for k in ('local_time', 'timezone')):
                raise ValueError('snooze requires explicit local_time and timezone')
            r = dict(old or {})
            r.update({k: v for k, v in args.items() if k not in ('expected_revision', 'late_window_seconds', 'followup_until_utc')})
            if ('local_time' in args or 'timezone' in args) and 'fold' not in args:
                r['fold'] = None
            due = local_instant(r['local_time'], r['timezone'], r.get('fold'))
            if due <= now:
                raise ValueError('scheduled time must be in the future; clarify past or ambiguous dates')
            late = args.get('late_window_seconds', old['expires'] - old['due'] if old else 3600)
            if type(late) not in (int, float) or not 1 <= late <= 604800:
                raise ValueError('late_window_seconds must be 1..604800')
            if not isinstance(r.get('title'), str) or not r['title']:
                raise ValueError('title must be a nonempty string')
            context = r.get('context', '')
            if not isinstance(context, str):
                raise ValueError('context must be a string')
            ref = r.get('source_ref')
            if ref is not None and (not isinstance(ref, str) or len(ref) > 500):
                raise ValueError('source_ref must be <=500 characters')
            mode = r.get('mode', 'fixed')
            if mode not in ('fixed', 'contextual'):
                raise ValueError('mode must be fixed or contextual')
            limit = r.get('followup_limit', 0)
            minimum = r.get('followup_min_seconds', 3600)
            deadline = r.get('followup_deadline')
            if 'followup_until_utc' in args:
                value = args['followup_until_utc']
                if not isinstance(value, str) or not value.endswith('Z') or 'T' not in value:
                    raise ValueError('followup_until_utc must be an explicit UTC ISO timestamp ending Z')
                deadline = datetime.fromisoformat(value[:-1] + '+00:00').timestamp()
            if type(limit) is not int or not 0 <= limit <= 7 or type(minimum) is not int or not 60 <= minimum <= 604800:
                raise ValueError('follow-up limit must be 0..7 and minimum interval 60..604800 seconds')
            if limit and (mode != 'contextual' or deadline is None or deadline <= due):
                raise ValueError('follow-up permission requires contextual mode and future deadline after due')
            if old and (limit > old['followup_limit'] or minimum < old['followup_min_seconds'] or
                        (deadline is not None and old['followup_deadline'] is not None and deadline > old['followup_deadline'])):
                raise ValueError('updates may only tighten stored follow-up permission')
            ident = old['id'] if old else str(uuid.uuid4())
            child = old and self.db.execute('SELECT parent_event_id FROM followups WHERE child_id=?', (ident,)).fetchone()
            expires = due + late
            if child:
                parent = self.reminder(self.event(child['parent_event_id'])['reminder_id'])
                if not self.lineage_valid(ident) or self.context_snapshot(old, context_reader).get('sendable') is not True:
                    raise ValueError('follow-up lineage or source is no longer relevant')
                if any(r.get(k) != old.get(k) for k in ('title','context','source_ref','mode')) or r['timezone'] != parent['timezone']:
                    raise ValueError('follow-up update must retain inherited purpose/source/mode and timezone')
                if due < max(now, parent['due']) + minimum:
                    raise ValueError('follow-up reschedule violates the stored minimum interval')
                if deadline is None or due >= deadline:
                    raise ValueError('follow-up must remain strictly before its stored deadline')
                expires = min(expires, deadline)
            self.db.execute('''INSERT OR REPLACE INTO reminders
                (id,revision,title,context,source_ref,local_time,timezone,fold,due,expires,status,
                 mode,followup_limit,followup_deadline,followup_min_seconds) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (ident, old['revision'] + 1 if old else 1, r['title'], context, ref,
                 r['local_time'], r['timezone'], r.get('fold'), due, expires, 'active', mode, limit, deadline, minimum))
            return self.reminder(ident)
        if validate_only:
            if operation_id is not None:raise ValueError('validation cannot create an operation receipt')
            self.db.execute('BEGIN IMMEDIATE')
            try:return mutate()
            finally:self.db.execute('ROLLBACK')
        return self.transaction(lambda:self.operation(operation_id,action,args,mutate))

    def lineage_valid(self, reminder_id):
        seen = set()
        for _ in range(8):
            link = self.db.execute('SELECT parent_event_id FROM followups WHERE child_id=?', (reminder_id,)).fetchone()
            if link is None:
                return True
            if reminder_id in seen:
                return False
            seen.add(reminder_id)
            e = self.event(link['parent_event_id'])
            r = self.reminder(e['reminder_id'])
            if r['revision'] != e['revision'] or r['status'] not in ('transport_confirmed', 'dismissed'):
                return False
            reminder_id = r['id']
        return False

    def recover(self):
        def do():
            self.db.execute("UPDATE events SET status='uncertain' WHERE status='submitting'")
            self.db.execute("UPDATE events SET delivery_status='uncertain' WHERE delivery_status='sending'")
        self.transaction(do)

    def due_events(self, now):
        def do():
            self.db.execute("""UPDATE reminders SET status='expired' WHERE id IN
                (SELECT id FROM reminders WHERE status='active' AND expires<=? ORDER BY expires,id LIMIT ?)""",
                (now,BATCH_SIZE))
            for row in self.db.execute('SELECT reminder_id FROM schedule_queue WHERE due<=? ORDER BY due,reminder_id LIMIT ?',
                                        (now,BATCH_SIZE)).fetchall():
                r=self.reminder(row['reminder_id'])
                if r['expires']<=now:
                    self.db.execute("UPDATE reminders SET status='expired' WHERE id=?",(r['id'],));continue
                if not self.lineage_valid(r['id']):
                    self.db.execute("UPDATE reminders SET status='invalidated' WHERE id=?", (r['id'],))
                    continue
                self.db.execute("INSERT OR IGNORE INTO events(event_id,reminder_id,revision,status) VALUES (?,?,?,'pending')",
                                (str(uuid.uuid4()), r['id'], r['revision']))
            output=[]
            mode_filter='' if self.contextual_enabled() else " AND dispatch_mode='fixed'"
            for e in self.db.execute("SELECT * FROM events WHERE status='pending'"+mode_filter+" AND retry_at<=? ORDER BY retry_at,event_id LIMIT ?",
                                     (now,BATCH_SIZE)).fetchall():
                view=self.tool('inspect',{'event_id':e['event_id']},now)
                r=view['reminder']
                if view['eligible']:output.append(dict(e))
                elif r['due']>now and r['status']=='active' and r['revision']==e['revision']:
                    self.db.execute('UPDATE events SET retry_at=MAX(retry_at,?) WHERE event_id=?',(r['due'],e['event_id']))
                else:self.db.execute("UPDATE events SET status='invalidated' WHERE event_id=?",(e['event_id'],))
            return output
        return self.transaction(do)

    def next_due(self):
        values=[]
        mode_filter='' if self.contextual_enabled() else " AND dispatch_mode='fixed'"
        for sql in ("SELECT due FROM schedule_queue ORDER BY due,reminder_id LIMIT 1",
                    "SELECT retry_at FROM events WHERE status='pending'"+mode_filter+" ORDER BY retry_at,event_id LIMIT 1",
                    "SELECT expires FROM reminders WHERE status='active' ORDER BY expires,id LIMIT 1"):
            row=self.db.execute(sql).fetchone()
            if row is not None:values.append(row[0])
        return min(values) if values else None

    def admit(self, event_id, adapter, now=None):
        now = time.time() if now is None else now
        def claim():
            view = self.tool('inspect', {'event_id': event_id}, now)
            if not view['eligible'] or view['event']['status'] != 'pending':
                return None
            self.db.execute("UPDATE events SET status='submitting', attempts=attempts+1 WHERE event_id=?", (event_id,))
            return view
        view = self.transaction(claim)
        if view is None:
            return None
        try:
            outcome = adapter({'event_id': event_id, 'reminder_id': view['reminder']['id'],
                'revision': view['reminder']['revision'], 'due_utc': view['reminder']['due'],
                'expires_utc': view['reminder']['expires'], 'source_ref': view['reminder']['source_ref'],
                'mode': view['reminder']['mode']})
        except Exception:
            outcome = {'status': 'uncertain'}
        def finish():
            status = outcome.get('status')
            attempt = self.event(event_id)['attempts']
            if status in ('unavailable', 'rejected'):
                status = 'pending' if status == 'unavailable' and attempt < 3 else 'failed'
            elif status != 'queued':
                status = 'uncertain'
            self.db.execute('UPDATE events SET status=?,retry_at=?,prompt_id=?,queued_behind=? WHERE event_id=?',
                (status, now + 30 * 2 ** (attempt - 1), outcome.get('prompt_id'), outcome.get('queued_behind'), event_id))
            return self.event(event_id)
        return self.transaction(finish)

    def complete_internal(self, args, now=None):
        """Acknowledge an agent wake without any messaging transport."""
        if not isinstance(args, dict) or set(args) != {'event_id', 'expected_revision'}:
            raise ValueError('complete takes event_id and expected_revision')
        def finish():
            event=self.event(args['event_id'])
            if type(args['expected_revision']) is not int or event['revision']!=args['expected_revision']:
                raise ValueError('stale wake revision')
            if event['delivery_status']=='handled_internal':
                return {'event_id':event['event_id'],'status':'handled_internal'}
            view=self.tool('inspect',{'event_id':args['event_id']},time.time() if now is None else now)
            if not view['eligible'] or event['status'] not in ('submitting','queued','uncertain') or event['delivery_status'] is not None:
                raise ValueError('wake is no longer eligible or already attempted')
            self.db.execute("UPDATE events SET delivery_status='handled_internal' WHERE event_id=?",(args['event_id'],))
            self.db.execute("UPDATE reminders SET status='handled_internal' WHERE id=?",(event['reminder_id'],))
            return {'event_id':event['event_id'],'status':'handled_internal'}
        return self.transaction(finish)

    def claimable(self, event_id, now, mode, expected_revision=None):
        view = self.tool('inspect', {'event_id': event_id}, now)
        e, r = view['event'], view['reminder']
        if r['mode'] != mode:
            raise ValueError('delivery action does not match stored reminder mode')
        if mode == 'contextual' and (type(expected_revision) is not int or expected_revision != r['revision']):
            raise ValueError('stale contextual reminder revision')
        if not view['eligible'] or e['status'] not in ('submitting', 'queued', 'uncertain') or e['delivery_status'] is not None:
            raise ValueError('event is cancelled, stale, expired, not admitted or already attempted')
        return view

    def context_snapshot(self, reminder, reader=None):
        if reminder['source_ref']:
            if reader is None:
                raise ValueError('source-bound contextual event requires configured source reader')
            return reader(reminder['source_ref'],reminder['id']) if reminder['source_ref'].startswith('note:') else reader(reminder['source_ref'])
        return {'fingerprint': stored_fingerprint(reminder), 'sendable': True, 'data_only': True}

    def check_context(self, reminder, fingerprint, reader=None, sending=True):
        snapshot = self.context_snapshot(reminder, reader)
        if not isinstance(fingerprint, str) or len(fingerprint) != 64 or snapshot.get('fingerprint') != fingerprint:
            raise ValueError('context changed or fingerprint invalid; recover current context and reconsider')
        if sending and snapshot.get('sendable') is not True:
            raise ValueError('source target is completed or cancelled; contextual send suppressed')
        return snapshot

    def deliver_contextual(self, event_id, expected_revision, text, source_fingerprint, sender,
                           context_reader=None, now=None):
        return self.deliver(event_id,sender,now,mode='contextual',expected_revision=expected_revision,
                            text=text,source_fingerprint=source_fingerprint,context_reader=context_reader)

    def deliver(self, event_id, sender, now=None, *, mode='fixed', expected_revision=None, text=None,
                source_fingerprint=None, context_reader=None):
        if mode == 'fixed' and text is not None:
            raise ValueError('fixed delivery cannot override stored text')
        if mode == 'contextual':
            if not isinstance(text, str) or not text.strip() or '\x00' in text:
                raise ValueError('contextual text must be nonblank, without NUL')
        def claim():
            instant = time.time() if now is None else now
            view = self.claimable(event_id, instant, mode, expected_revision)
            r = view['reminder']
            if mode == 'contextual':
                self.check_context(r, source_fingerprint, context_reader)
                body = text
            else:
                body = '[Reminder] ' + r['title'] + ('\n' + r['context'] if r['context'] else '')
            if now is None and not r['due'] <= time.time() < r['expires']:
                raise ValueError('relevance changed during context recovery; do not send')
            self.db.execute("UPDATE events SET delivery_status='sending',body_sha256=?,source_fingerprint=? WHERE event_id=?",
                            (hashlib.sha256(body.encode()).hexdigest(), source_fingerprint, event_id))
            return r, body
        r, body = self.transaction(claim)
        # Durable marker blocks mutations/retries; external source can still change after this snapshot.
        try:
            outcome = sender(body)
        except Exception:
            outcome = {'status': 'uncertain'}
        def finish():
            status = outcome.get('status')
            if status not in ('transport_confirmed', 'rejected'):
                status = 'uncertain'
            self.db.execute('UPDATE events SET delivery_status=?,wire_id=? WHERE event_id=?',
                            (status, outcome.get('wire_id'), event_id))
            if status == 'transport_confirmed':
                self.db.execute("UPDATE reminders SET status='transport_confirmed' WHERE id=? AND revision=?", (r['id'], r['revision']))
            return self.event(event_id)
        return self.transaction(finish)

    def dismiss_contextual(self, event_id, expected_revision, reason, source_fingerprint=None,
                           context_reader=None, now=None):
        if reason not in ('no_update', 'not_relevant', 'context_missing'):
            raise ValueError('dismiss reason must be no_update, not_relevant or context_missing')
        def decide():
            view = self.claimable(event_id, time.time() if now is None else now, 'contextual', expected_revision)
            r = view['reminder']
            if reason == 'context_missing':
                try:
                    self.context_snapshot(r, context_reader)
                except (ValueError, OSError, sqlite3.Error):
                    raise ValueError('source unavailable: hold unsent until repair or expiry; do not fabricate a fingerprint') from None
                else:
                    raise ValueError('context_missing requires an actual source read failure')
            else:
                self.check_context(r, source_fingerprint, context_reader, sending=False)
                fingerprint = source_fingerprint
            self.db.execute("UPDATE events SET delivery_status='dismissed',decision_reason=?,source_fingerprint=? WHERE event_id=?",
                            (reason, fingerprint, event_id))
            self.db.execute("UPDATE reminders SET status='dismissed' WHERE id=?", (r['id'],))
            return self.event(event_id)
        return self.transaction(decide)

    def create_followup(self, args, now=None, context_reader=None,operation_id=None,validate_only=False):
        now = time.time() if now is None else now
        required = {'parent_event_id', 'expected_revision', 'local_time', 'timezone'}
        if not isinstance(args, dict) or not required <= set(args) or set(args) - (required | {'fold', 'late_window_seconds','parent_reminder_id'}):
            raise ValueError('follow-up takes only parent event/revision and explicit schedule')
        def create():
            if not self.contextual_enabled():raise ValueError('contextual follow-ups held by operator policy')
            e = self.event(args['parent_event_id'])
            r = self.reminder(e['reminder_id'])
            if 'parent_reminder_id' in args and args['parent_reminder_id']!=r['id']:
                raise ValueError('follow-up parent binding mismatch')
            if type(args['expected_revision']) is not int or r['revision'] != args['expected_revision'] or r['revision'] != e['revision']:
                raise ValueError('stale follow-up parent revision')
            if r['mode'] != 'contextual' or r['status'] not in ('transport_confirmed', 'dismissed') or not self.lineage_valid(r['id']):
                raise ValueError('follow-up parent is cancelled, revised or not terminal contextual')
            if e['delivery_status'] != 'transport_confirmed' and not (e['delivery_status'] == 'dismissed' and e['decision_reason'] == 'no_update'):
                raise ValueError('follow-up requires confirmed contextual send or no_update decision')
            late = args.get('late_window_seconds', r['expires'] - r['due'])
            request = {'local_time': args['local_time'], 'timezone': args['timezone'], 'fold': args.get('fold'), 'late': late}
            digest = hashlib.sha256(json.dumps(request, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            existing = self.db.execute('SELECT * FROM followups WHERE parent_event_id=?', (e['event_id'],)).fetchone()
            if existing:
                if existing['request_fingerprint'] != digest:
                    raise ValueError('parent already has a different follow-up; do not recreate it')
                return self.reminder(existing['child_id'])
            if self.context_snapshot(r, context_reader).get('sendable') is not True:
                raise ValueError('source is completed or cancelled; no automatic follow-up')
            due = local_instant(args['local_time'], args['timezone'], args.get('fold'))
            if args['timezone'] != r['timezone']:
                raise ValueError('follow-up must retain the parent confirmed timezone')
            if type(late) not in (int, float) or not 1 <= late <= 604800:
                raise ValueError('late_window_seconds must be 1..604800')
            if r['followup_limit'] <= 0 or r['followup_deadline'] is None or now >= r['expires'] or due >= r['followup_deadline']:
                raise ValueError('follow-up permission is absent, exhausted or outside its deadline/relevance')
            if due < max(now, r['due']) + r['followup_min_seconds']:
                raise ValueError('follow-up violates the stored minimum interval')
            child = str(uuid.uuid4())
            self.db.execute('''INSERT INTO reminders
                (id,revision,title,context,source_ref,local_time,timezone,fold,due,expires,status,
                 mode,followup_limit,followup_deadline,followup_min_seconds)
                SELECT ?,1,title,context,source_ref,?,?,?, ?,?,'active',mode,followup_limit-1,
                 followup_deadline,followup_min_seconds FROM reminders WHERE id=?''',
                (child,args['local_time'],args['timezone'],args.get('fold'),due,min(due+late,r['followup_deadline']),r['id']))
            self.db.execute('INSERT INTO followups VALUES (?,?,?)', (e['event_id'],child,digest))
            return self.reminder(child)
        if validate_only:
            if operation_id is not None:raise ValueError('validation cannot commit receipts')
            self.db.execute('BEGIN IMMEDIATE')
            try:return create()
            finally:self.db.execute('ROLLBACK')
        return self.transaction(lambda:self.operation(operation_id,'create_followup',args,create))

def stored_fingerprint(reminder):
    data = {k: reminder[k] for k in ('id','revision','mode','title','context','source_ref')}
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def budget_module():
    spec=importlib.util.spec_from_file_location('personal_assistant_budget',Path(__file__).with_name('context_budget.py'))
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result

def mutate_with_source_journal(store,cfg,action,args,now=None):
    """Installed schema2 source-bound timer mutations cannot bypass source ACKs."""
    data=dict(args);op=data.pop('operation_id',None);source_revision=data.pop('expected_source_revision',None)
    old=None if action=='create' else store.reminder(data.get('id'))
    if old is not None and 'source_ref' in data and data['source_ref']!=old['source_ref']:
        raise ValueError('source binding is immutable; use a separately reviewed new intake')
    ref=data.get('source_ref') if old is None else old['source_ref']
    if cfg.get('source_schema',1)!=2 or ref is None:
        if op is not None or source_revision is not None:raise ValueError('journal parameters require an indexed source-bound reminder')
        return store.tool(action,data,now,context_reader=source_reader(cfg))
    if op is None or type(source_revision) is not int or source_revision<1 or not cfg.get('source_journal'):
        raise ValueError('source-bound mutation requires immutable operation_id and expected_source_revision')
    # Reject invalid dates/arguments before freezing the source. All temporary
    # validation writes are rolled back; no event, receipt or timer commits.
    if store.receipt(op) is None:store.tool(action,data,now,context_reader=source_reader(cfg),validate_only=True)
    path=Path(__file__).with_name('source_store.py')
    sys.path.insert(0,str(path.parent))
    spec=importlib.util.spec_from_file_location('source_schedule_writer',path)
    source=importlib.util.module_from_spec(spec);spec.loader.exec_module(source)
    db=source.connect(cfg['source_db'])
    try:
        if source.meta(db,'namespace')!=cfg.get('source_namespace'):raise ValueError('source schedule namespace mismatch')
        spec=importlib.util.spec_from_file_location('source_schedule_journal',Path(__file__).with_name('schedule_journal.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        journal=module.Journal(cfg['source_journal'])
        try:
            journal.request(op,ref,source_revision,action,data)
            return journal.run(op,store,source.SourceSchedule(db),now)['result']
        finally:journal.db.close()
    finally:db.close()

def followup_with_source_journal(store,cfg,args,now=None):
    allowed={'parent_event_id','expected_revision','local_time','timezone','fold','late_window_seconds'}
    if not isinstance(args,dict) or set(args)-allowed:raise ValueError('invalid follow-up input')
    parent=store.reminder(store.event(args['parent_event_id'])['reminder_id'])
    if cfg.get('source_schema',1)!=2 or parent['source_ref'] is None:
        return store.create_followup(args,now,source_reader(cfg))
    if not cfg.get('source_journal'):raise ValueError('source journal required')
    # One immutable child request per actual parent event. No caller identity
    # can reset this ID; conflicting schedules remain held for reconciliation.
    op='followup-'+hashlib.sha256(args['parent_event_id'].encode()).hexdigest()
    request_args={**args,'parent_reminder_id':parent['id']}
    path=Path(__file__).with_name('source_store.py');sys.path.insert(0,str(path.parent))
    spec=importlib.util.spec_from_file_location('followup_source',path)
    source=importlib.util.module_from_spec(spec);spec.loader.exec_module(source)
    spec=importlib.util.spec_from_file_location('followup_journal',Path(__file__).with_name('schedule_journal.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    journal=module.Journal(cfg['source_journal']);db=None
    try:
        previous=journal.db.execute('SELECT request_json FROM operations WHERE operation_id=?',(op,)).fetchone()
        if previous is not None:revision=json.loads(previous[0])['expected_source_revision']
        else:
            store.create_followup(request_args,now,source_reader(cfg),validate_only=True)
            snapshot=source_reader(cfg)(parent['source_ref'],parent['id']) if parent['source_ref'].startswith('note:') else source_reader(cfg)(parent['source_ref']);revision=snapshot['revision']
        db=source.connect(cfg['source_db'])
        if source.meta(db,'namespace')!=cfg.get('source_namespace'):raise ValueError('source namespace mismatch')
        journal.request(op,parent['source_ref'],revision,'create_followup',request_args)
        return journal.run(op,store,source.SourceSchedule(db),now,context_reader=source_reader(cfg))['result']
    finally:
        if db is not None:db.close()
        journal.db.close()

def action_summary(result,followup=False):
    if followup:
        return {'id':result['id'],'revision':result['revision'],'status':result['status'],'scheduled':True}
    return {k:result[k] for k in ('event_id','status','delivery_status') if k in result}

def reminder_summary(result,store):
    output={k:result[k] for k in ('id','revision','local_time','timezone','due','expires','status','mode')}
    output['schedule_link_pending']=bool(store.db.execute('SELECT 1 FROM reminder_operation_holds WHERE reminder_id=?',(result['id'],)).fetchone())
    return output

def source_reader(cfg):
    def read(ref,reminder_id=None):
        if bool(cfg.get('source_db')) == bool(cfg.get('source_json')):
            raise ValueError('source-bound mode requires exactly one private source_db or source_json')
        legacy = bool(cfg.get('source_json'))
        if legacy:
            namespace = cfg.get('source_namespace')
            if not isinstance(namespace, str) or str(uuid.UUID(namespace)) != namespace:
                raise ValueError('legacy JSON source requires a stable configured namespace UUID')
        schema=cfg.get('source_schema',1)
        if type(schema) is not int or schema not in (1,2):raise ValueError('unsupported configured source schema')
        path = Path(__file__).with_name('legacy_source.py' if legacy else ('source_store.py' if schema==2 else 'source_store_v1.py'))
        spec = importlib.util.spec_from_file_location('personal_assistant_source', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if legacy:
            return module.read_context(cfg['source_json'], ref, namespace)
        result=module.read_context(cfg['source_db'], ref)
        if ref.startswith('note:'):
            if schema!=2 or reminder_id is None:raise ValueError('note due requires exact ledger binding')
            db=module.connect(cfg['source_db'],readonly=True)
            try:
                with module.atomic(db),module.bounded_work(db):
                    # Rehydrate again inside the binding snapshot: no source/binding time split.
                    result=module.context_snapshot(db,ref)
                    mapping=db.execute('SELECT * FROM reminder_mappings WHERE reminder_id=?',(reminder_id,)).fetchone()
                    note=module._row(db,'note',ref.split(':',1)[1])
                    bound=(mapping is not None and mapping['source_ref']==ref and
                           mapping['source_revision']==note['revision'] and mapping['source_digest']==note['original_digest'])
                    result['sendable']=result['sendable'] and bound
                    result['note_timer_binding_current']=bool(bound)
                    result['fingerprint']=module.digest({'snapshot':result,'bound_reminder_id':reminder_id})
            finally:db.close()
        if schema==2 and result.get('namespace')!=cfg.get('source_namespace'):
            raise ValueError('configured source namespace mismatch')
        return result
    return read

def load_config(path):
    path=Path(path)
    if not path.is_absolute():raise ValueError('absolute private host config required')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=0o600:
            raise ValueError('private regular host config required')
        with os.fdopen(fd,'rb',closefd=False) as file:raw=file.read(65537)
        if len(raw)>65536:raise ValueError('host config exceeds bound')
    finally:os.close(fd)
    cfg = strict_json(raw)
    if not isinstance(cfg,dict):raise ValueError('host config must be an object')
    if type(cfg.get('contextual_enabled',True)) is not bool:raise ValueError('contextual_enabled must be boolean')
    required = ('database', 'agent', 'expected_agent_cid', 'agent_state_dir', 'fleet_control_module')
    if any(not cfg.get(k) for k in required):
        raise ValueError('host config missing required fields')
    if cfg.get('source_db') and cfg.get('source_json'):
        raise ValueError('configure only one authoritative source backend')
    if cfg.get('source_json'):
        namespace = cfg.get('source_namespace')
        if not isinstance(namespace, str) or str(uuid.UUID(namespace)) != namespace:
            raise ValueError('legacy JSON source requires a stable configured namespace UUID')
    return cfg

def validate_delivery_route(cfg):
    if not cfg.get('fleet_cli'):
        raise ValueError('outbound delivery requires configured Fleet CLI')
    route = cfg.get('route')
    if not isinstance(route, dict):
        raise ValueError('outbound delivery requires an explicitly configured route')
    if route.get('verified') is not True or route.get('channel') not in ('ours', 'telegram'):
        raise ValueError('route must be explicitly verified')
    cid = route.get('contact_cid', '')
    if len(cid) != 64 or any(c not in '0123456789abcdefABCDEF' for c in cid):
        raise ValueError('exact verified contact CID required')
    if route['channel'] == 'telegram':
        if route.get('chat_type') != 'private' or not isinstance(route.get('chat_id'), str) or not route['chat_id'].isdigit() or int(route['chat_id']) <= 0:
            raise ValueError('Telegram requires a pinned nonzero private Owner chat')
        if route.get('telegram_user_id') != route['chat_id'] or not route.get('bot_name'):
            raise ValueError('verified Telegram Owner user/chat binding required')
    return cfg

def fleet_adapter(cfg, event):
    r = subprocess.run([cfg.get('node', 'node'), str(Path(__file__).with_name('fleet_due.mjs'))],
        input=json.dumps({'config': cfg, 'event': event}), text=True, capture_output=True, timeout=15)
    if r.returncode != 0:
        return {'status': 'uncertain'}
    return json.loads(r.stdout)

def fleet_sender(cfg, text):
    # Outbound delivery is optional and separately configured from self-wakes.
    try:
        validate_delivery_route(cfg)
        selected = subprocess.run([cfg['fleet_cli'], 'ours', 'tools', cfg['agent']],
            text=True, capture_output=True, timeout=15)
        if selected.returncode != 0:
            return {'status': 'rejected'}
        identity = json.loads(selected.stdout).get('identity', {})
        if identity.get('cid', '').upper() != cfg['expected_agent_cid'].upper():
            return {'status': 'rejected'}
    except Exception:
        return {'status': 'rejected'}
    fd, name = tempfile.mkstemp(prefix='.send-', dir=Path(cfg['database']).parent)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump({'contact': cfg['route']['contact_cid'], 'text': text}, f)
        r = subprocess.run([cfg['fleet_cli'], 'ours', 'call', cfg['agent'], 'send_message', '--args-file', name],
            text=True, capture_output=True, timeout=25)
        if r.returncode != 0:
            return {'status': 'uncertain'}
        body = json.loads(r.stdout)
        result = body.get('result', {})
        out = result.get('structuredContent', {}).get('outcome', {})
        if out.get('sent') is True and (out.get('wire_id') or out.get('wireId')):
            return {'status': 'transport_confirmed', 'wire_id': out.get('wire_id') or out.get('wireId')}
        return {'status': 'uncertain'}
    finally:
        os.unlink(name)

def serve(store, cfg):
    # One scheduler per DB. Deterministic DB/clock checks are not model turns.
    lock = open(cfg['database'] + '.scheduler.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    store.recover()
    while True:
        now = time.time()
        for e in store.due_events(now):
            store.admit(e['event_id'], lambda event: fleet_adapter(cfg, event))
        due=store.next_due()
        delay = 30 if due is None else max(0.1, min(30, due - time.time()))
        time.sleep(delay)

def main():
    os.umask(0o077)
    p = argparse.ArgumentParser()
    p.add_argument('--config', required=True)
    p.add_argument('action', choices=['create','list','update','cancel','snooze','inspect','deliver',
                                    'deliver_contextual','dismiss_contextual','create_followup','complete','serve'])
    opts = p.parse_args()
    cfg = load_config(opts.config)
    if opts.action in ('deliver', 'deliver_contextual'):
        validate_delivery_route(cfg)  # Before any claim or ledger mutation.
    store = Store(cfg['database'])
    store.configure_policy(cfg.get('contextual_enabled',True))
    if opts.action == 'serve':
        serve(store, cfg)
        return
    raw=sys.stdin.buffer.read()
    args = {} if opts.action=='list' and not raw.strip() else strict_json(raw)
    if not isinstance(args, dict):
        raise ValueError('input must be a JSON object')
    if opts.action == 'complete':
        print(canonical(store.complete_internal(args)));return
    budget=budget_module()
    if opts.action=='inspect':
        if set(args)!={'event_id'}:raise ValueError('inspect takes only event_id')
        result=store.inspect_budgeted(args['event_id'],budget,source_reader(cfg))
        if result is None:raise SystemExit(2)  # Exhausted ticket: no additional model payload.
        print(canonical(result));return
    event_action=opts.action in ('deliver','deliver_contextual','dismiss_contextual','create_followup')
    if event_action:
        event_id=args.get('parent_event_id') if opts.action=='create_followup' else args.get('event_id')
        reserved=store.reserve_action_response(event_id,budget)
        if reserved is not True:
            if isinstance(reserved,dict):print(canonical(reserved))
            raise SystemExit(2)
    if opts.action == 'deliver':
        if set(args) != {'event_id'}:
            raise ValueError('deliver takes only event_id; host route is fixed')
        result = store.deliver(args['event_id'], lambda text: fleet_sender(cfg, text))
    elif opts.action == 'deliver_contextual':
        if set(args) != {'event_id','expected_revision','text','source_fingerprint'}:
            raise ValueError('contextual delivery takes event/revision/text/fingerprint only; host route is fixed')
        result = store.deliver_contextual(args['event_id'],args['expected_revision'],args['text'],
                                          args['source_fingerprint'],lambda text: fleet_sender(cfg,text),source_reader(cfg))
    elif opts.action == 'dismiss_contextual':
        if not {'event_id','expected_revision','reason'} <= set(args) or set(args) - {'event_id','expected_revision','reason','source_fingerprint'}:
            raise ValueError('contextual dismissal takes event/revision/reason/fingerprint only')
        result = store.dismiss_contextual(args['event_id'],args['expected_revision'],args['reason'],
                                          args.get('source_fingerprint'),source_reader(cfg))
    elif opts.action == 'create_followup':
        result = followup_with_source_journal(store,cfg,args)
    else:
        if opts.action=='list':
            result=store.public_list(args,budget)
        else:
            result=reminder_summary(mutate_with_source_journal(store,cfg,opts.action,args),store)
    if event_action:
        result=action_summary(result,opts.action=='create_followup')
        def account_response():
            ticket=store.recovery_budget(event_id,budget)
            if ticket is None:raise ValueError('stopped action ticket')
            ticket.admit(result,whole_slot='control')
            store.save_recovery_budget(event_id,ticket)
        store.transaction(account_response)
    else:
        try:budget.TurnBudget().admit(result,whole_slot='control')
        except budget.ContextOverflow:result=budget.failure('context_overflow')
    print(canonical(result))

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        # No raw private source values/SQL/paths in a model-visible exception.
        print(canonical({'status':'operation_failed','hold':True,'data_only':True}), file=sys.stderr)
        sys.exit(1)
