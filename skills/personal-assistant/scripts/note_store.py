#!/usr/bin/env python3
"""Private canonical notes and derived local index. Never retains to shared memory or sends."""
from datetime import datetime, timezone
import json
import math
import re
import sqlite3
import struct
import time

import source_store as s
from context_budget import ContextOverflow, bounded_result, compact_json, fits

VERSION = '1'
MAX_CANDIDATES = 2048
DIMENSIONS = 384
TABLES = ('notes','note_revisions','note_vectors','note_tags','note_rules','note_rule_terms','note_rule_outcomes','return_decisions','task_trigger_reservations','task_trigger_tickets','message_return_tickets')
SCHEMA = '''
CREATE TABLE notes(
 id TEXT PRIMARY KEY,capture_key TEXT NOT NULL UNIQUE,revision INTEGER NOT NULL CHECK(revision>0),status TEXT NOT NULL CHECK(status IN ('active','resolved','archived','forgotten')),
 archived INTEGER NOT NULL CHECK(archived IN (0,1)),updated_utc TEXT NOT NULL,
 original_json TEXT NOT NULL,original_digest TEXT NOT NULL,semantic_state TEXT NOT NULL CHECK(semantic_state IN ('queued','completed','failed','unavailable','excluded')));
CREATE INDEX note_status_time_id ON notes(status,updated_utc,id);
CREATE TABLE note_revisions(id TEXT NOT NULL,revision INTEGER NOT NULL,original_json TEXT NOT NULL,original_digest TEXT NOT NULL,
 updated_utc TEXT NOT NULL,PRIMARY KEY(id,revision));
CREATE TABLE note_vectors(id TEXT PRIMARY KEY REFERENCES notes(id),revision INTEGER NOT NULL,digest TEXT NOT NULL,model TEXT NOT NULL,vector BLOB NOT NULL CHECK(length(vector)=1536));
CREATE TABLE note_tags(tag TEXT NOT NULL,id TEXT NOT NULL REFERENCES notes(id),PRIMARY KEY(tag,id));
CREATE VIRTUAL TABLE note_fts USING fts5(id UNINDEXED,text,summary,tokenize='unicode61');
CREATE TABLE note_rules(id TEXT PRIMARY KEY,purpose_key TEXT NOT NULL UNIQUE,source_ref TEXT NOT NULL,revision INTEGER NOT NULL,invalidated_by TEXT,
 active INTEGER NOT NULL CHECK(active IN (0,1)),remaining INTEGER NOT NULL CHECK(remaining BETWEEN 0 AND 7),
 deadline TEXT NOT NULL,min_seconds INTEGER NOT NULL CHECK(min_seconds BETWEEN 60 AND 604800),last_at TEXT,
 original_json TEXT NOT NULL);
CREATE INDEX note_rule_source ON note_rules(source_ref,active);
CREATE TABLE note_rule_terms(term TEXT NOT NULL,rule_id TEXT NOT NULL REFERENCES note_rules(id),PRIMARY KEY(term,rule_id));
CREATE TABLE note_rule_outcomes(rule_id TEXT NOT NULL REFERENCES note_rules(id),wire_id TEXT NOT NULL,note_revision INTEGER NOT NULL,
 note_digest TEXT NOT NULL,reserved_utc TEXT NOT NULL,purpose_digest TEXT NOT NULL,PRIMARY KEY(rule_id,wire_id));
'''
from return_tools import SCHEMA as RETURN_SCHEMA
SCHEMA += RETURN_SCHEMA
from task_triggers import SCHEMA as TASK_TRIGGER_SCHEMA
SCHEMA += TASK_TRIGGER_SCHEMA
from task_trigger_session import SCHEMA as TASK_TICKET_SCHEMA
SCHEMA += TASK_TICKET_SCHEMA
from message_return_session import SCHEMA as MESSAGE_TICKET_SCHEMA
SCHEMA += MESSAGE_TICKET_SCHEMA


def signature(db):
    names = set(TABLES) | {'note_fts','note_fts_data','note_fts_idx','note_fts_content','note_fts_docsize','note_fts_config',
                          'note_status_time_id','note_rule_source'}
    return [r for r in s._schema_signature(db) if r[1] in names]


def expected_signature():
    db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
    for statement in SCHEMA.split(';'):
        if statement.strip():db.execute(statement)
    value=signature(db);db.close();return value


def check(db):
    if s.meta(db,'notes_schema') != VERSION or signature(db) != expected_signature():
        raise ValueError('unrecognized notes extension')


def extend(db, namespace, expected_state_digest):
    """Operator only under fresh pause/backup. Existing state equality and exact schema."""
    with s.atomic(db,True):
        s.check_schema(db)
        if s.meta(db,'namespace') != namespace or s.digest(s.export_state(db)) != expected_state_digest:
            raise ValueError('current source checkpoint mismatch')
        if db.execute("SELECT 1 FROM metadata WHERE key='notes_schema'").fetchone():
            check(db);return {'status':'already_extended','notes_schema':VERSION,'data_only':True}
        baseline=sqlite3.connect(':memory:');baseline.row_factory=sqlite3.Row
        for statement in s.SCHEMA.split(';'):
            if statement.strip():baseline.execute(statement)
        expected=s._schema_signature(baseline);baseline.close()
        if s._schema_signature(db) != expected:
            raise ValueError('unknown or partial source schema')
        for statement in SCHEMA.split(';'):
            if statement.strip():db.execute(statement)
        db.executemany('INSERT INTO metadata VALUES (?,?)',[('notes_schema',VERSION),('notes_generation','0')])
        check(db)
    return {'status':'extended','notes_schema':VERSION,'data_only':True}


def utc(value):
    if not isinstance(value,str) or not value.endswith('Z') or len(value)>32:
        raise ValueError('explicit UTC instant required')
    return datetime.fromisoformat(value[:-1]+'+00:00').isoformat(timespec='microseconds').replace('+00:00','Z')


def text(value,maximum):
    if not isinstance(value,str) or not value.strip() or '\x00' in value or (maximum is not None and len(value.encode())>maximum):
        raise ValueError('invalid bounded note field')
    return value


def valid(record):
    required={'id','text','summary','importance','importance_basis','tags','source','source_wire_id','source_part','observed_utc','status'}
    if not isinstance(record,dict) or not required<=set(record):
        raise ValueError('complete curated note required')
    s.identifier(record['id']);text(record['text'],None);text(record['summary'],None)
    text(record['source'],None);text(record['source_wire_id'],128);text(record['source_part'],64);utc(record['observed_utc'])
    if type(record['importance']) is not int or not 0<=record['importance']<=3:
        raise ValueError('importance must be 0..3')
    if record['importance_basis'] not in ('owner','inferred','unspecified'):
        raise ValueError('importance provenance required')
    tags=record['tags']
    if (not isinstance(tags,list) or len(tags)>8 or len(set(tags))!=len(tags)
            or any(not isinstance(t,str) or not re.fullmatch(r'[\w-]{1,32}',t) for t in tags)):
        raise ValueError('bounded literal tags required')
    if record['status'] not in ('active','resolved','archived','forgotten'):
        raise ValueError('invalid note status')
    return record


def row(db,key):
    r=db.execute('SELECT * FROM notes WHERE id=?',(s.identifier(key),)).fetchone()
    if r is None:raise ValueError('unknown note')
    return r


def snapshot(db,ref):
    check(db)
    key=s.identifier(ref.split(':',1)[1]);r=row(db,key)
    hidden=r['status'] in ('archived','forgotten')
    record={'id':key,'revision':r['revision'],'status':r['status'],'content_excluded':True} if hidden else json.loads(r['original_json'])|{'revision':r['revision']}
    out={'namespace':s.meta(db,'namespace'),'ref':ref,'kind':'note','root_id':key,'records':[record],
         'dependent_ids':[],'sendable':r['status']=='active','schema_version':2,'backend':'indexed_sqlite_v2',
         'revision':r['revision'],'semantic_state':r['semantic_state'],'data_only':True}
    out['fingerprint']=s.digest({'snapshot':out,'complete_source_digest':r['original_digest']})
    return bounded_result(out)


def summary(r,db=None):
    record=json.loads(r['original_json'])
    origin=record['source_wire_id']
    if db is not None:
        first=db.execute('SELECT original_json FROM note_revisions WHERE id=? AND revision=1',(r['id'],)).fetchone()
        if first is None:raise ValueError('original note provenance missing')
        origin=json.loads(first[0])['source_wire_id']
    return {'ref':'note:'+r['id'],'revision':r['revision'],'digest':r['original_digest'],
            'summary':record['summary'],'representation':'curated_summary','status':r['status'],
            'importance':record['importance'],'importance_basis':record['importance_basis'],
            'observed_utc':record['observed_utc'],'source':record['source'],'source_wire_id':record['source_wire_id'],'source_part':record['source_part'],
            'semantic_state':r['semantic_state'],'origin_source_wire_id':origin}


def bump(db):
    db.execute("UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='notes_generation'")
    s.bump(db)


def put(db,action,data):
    key=s.identifier(data.get('id'));expected=data.get('expected_revision')
    if type(expected) is not int or expected<0:raise ValueError('expected note revision required')
    old=db.execute('SELECT * FROM notes WHERE id=?',(key,)).fetchone()
    if (old is None and expected!=0) or (old is not None and old['revision']!=expected):
        raise ValueError('note revision conflict')
    if db.execute("SELECT 1 FROM source_schedule_operations WHERE source_ref=? AND status='prepared' LIMIT 1",('note:'+key,)).fetchone():
        raise ValueError('pending note schedule operation')
    if action=='note_put':
        if set(data)-{'id','expected_revision','record','changes'} or ('record' in data)==('changes' in data):
            raise ValueError('record or changes required')
        if old is None:
            record=valid(data['record'])
            if record['status']!='active':raise ValueError('new notes must be active')
            # Same actual incoming source may be captured only once (retries use exact ID/CAS).
            if db.execute('SELECT 1 FROM notes WHERE capture_key=?',(s.digest([record['source_wire_id'],record['source_part']]),)).fetchone():
                raise ValueError('source already captured; recover exact prior note')
        else:
            if 'changes' not in data or not isinstance(data['changes'],dict) or set(data['changes'])&{'id','status'}:
                raise ValueError('existing note changes preserve identity; use explicit lifecycle action')
            if old['status'] in ('archived','forgotten'):raise ValueError('explicit restore required')
            record=valid(json.loads(old['original_json'])|data['changes'])
        if record['id']!=key:raise ValueError('note ID mismatch')
    else:
        if old is None or set(data)!={'id','expected_revision'}:raise ValueError('exact note lifecycle mutation required')
        record=json.loads(old['original_json'])
        record['status']={'note_forget':'forgotten','note_archive':'archived','note_restore':'active','note_resolve':'resolved'}[action]
        if action=='note_restore' and old['status'] not in ('archived','forgotten','resolved'):
            raise ValueError('note not excluded/resolved')
    now=s.utc_now();rev=expected+1;sha=s.digest(record);hidden=record['status'] in ('archived','forgotten')
    db.execute('''INSERT INTO notes VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
      revision=excluded.revision,status=excluded.status,archived=excluded.archived,updated_utc=excluded.updated_utc,
      original_json=excluded.original_json,original_digest=excluded.original_digest,semantic_state=excluded.semantic_state''',
      (key,old['capture_key'] if old is not None else s.digest([record['source_wire_id'],record['source_part']]),rev,record['status'],int(hidden),now,compact_json(record),sha,'excluded' if hidden else 'queued'))
    db.execute('INSERT INTO note_revisions VALUES (?,?,?,?,?)',(key,rev,compact_json(record),sha,now))
    db.execute('DELETE FROM note_vectors WHERE id=?',(key,));db.execute('DELETE FROM note_fts WHERE id=?',(key,))
    db.execute('DELETE FROM note_tags WHERE id=?',(key,))
    if not hidden:
        db.execute('INSERT INTO note_fts(id,text,summary) VALUES (?,?,?)',(key,record['text'],record['summary']))
        db.executemany('INSERT INTO note_tags VALUES (?,?)',[(tag,key) for tag in record['tags']])
    # A correction or lifecycle change requires a fresh explicit rule; old intent never silently rebinds.
    invalidate_rules(db,'note:'+key,action,terminal=action!='note_put')
    bump(db)
    return bounded_result({'status':'source_committed','ref':'note:'+key,'revision':rev,'digest':sha,
      'semantic_state':'excluded' if hidden else 'queued','timer_created':False,'data_only':True,
      'retention':'private_revisions_retained' if hidden else 'private_canonical'})


def invalidate_rules(db,ref,action,terminal=False):
    db.execute('DELETE FROM note_rule_terms WHERE rule_id IN (SELECT id FROM note_rules WHERE source_ref=?)',(ref,))
    # Exclusion must also close a previously correction-disabled continuation.
    # A later ordinary correction must never overwrite that terminal reason.
    predicate='remaining>0' if terminal else 'active=1'
    reason='terminal:'+action if terminal and action in ('note_put','task_put') else action
    db.execute('UPDATE note_rules SET active=0,revision=revision+1,invalidated_by=? WHERE source_ref=? AND '+predicate,(reason,ref))


def vector(value):
    if not isinstance(value,list) or len(value)!=DIMENSIONS or any(type(v) not in (int,float) or not math.isfinite(v) for v in value):
        raise ValueError('invalid local embedding')
    norm=math.sqrt(sum(v*v for v in value))
    if not 0.9<=norm<=1.1:raise ValueError('normalized local embedding required')
    return struct.pack('<384f',*value)


def index(db,data,embed):
    if set(data)!={'id','expected_revision'}:raise ValueError('exact indexing request required')
    r=row(db,data['id'])
    if type(data['expected_revision']) is not int or r['revision']!=data['expected_revision']:
        raise ValueError('note index revision conflict')
    if r['status'] in ('forgotten','archived'):raise ValueError('excluded note cannot be indexed')
    record=json.loads(r['original_json'])
    try:
        if embed is None:raise OSError('offline index not configured')
        model,values=embed('passage',[record['summary']]);blob=vector(values[0])
        text(model,128)
    except (OSError,ValueError,TimeoutError):
        state='unavailable' if embed is None else 'failed'
        db.execute('UPDATE notes SET semantic_state=? WHERE id=?',(state,r['id']))
        db.execute('DELETE FROM note_vectors WHERE id=?',(r['id'],));bump(db)
        return {'status':'source_committed','ref':'note:'+r['id'],'revision':r['revision'],'semantic_state':state,'data_only':True}
    db.execute('INSERT OR REPLACE INTO note_vectors VALUES (?,?,?,?,?)',(r['id'],r['revision'],r['original_digest'],model,blob))
    db.execute("UPDATE notes SET semantic_state='completed' WHERE id=?",(r['id'],));bump(db)
    return {'status':'source_committed','ref':'note:'+r['id'],'revision':r['revision'],'semantic_state':'completed',
            'searchable_current_revision':True,'index_kind':'local_summary_embedding','data_only':True}


def filters(data):
    where=["n.status IN ('active','resolved')"];params=[]
    tag=data.get('tag')
    if tag is not None:
        if not isinstance(tag,str) or not re.fullmatch(r'[\w-]{1,32}',tag):raise ValueError('literal tag required')
        where.append('n.id IN (SELECT id FROM note_tags WHERE tag=?)');params.append(tag)
    for key,op in [('start','>='),('end','<')]:
        if key in data:where.append('n.updated_utc'+op+'?');params.append(utc(data[key]))
    return where,params


def page(db,data,search=False):
    allowed={'limit','cursor','tag','start','end'}|({'query'} if search else set())
    if set(data)-allowed:raise ValueError('unsupported note page field')
    limit=data.get('limit',5)
    if type(limit) is not int or not 1<=limit<=25:raise ValueError('note page limit must be 1..25')
    where,params=filters(data);join='';ns=s.meta(db,'namespace');gen=int(s.meta(db,'notes_generation'))
    scope=s.digest({k:v for k,v in data.items() if k not in ('cursor','limit')})
    if search:
        query=text(data.get('query'),None);terms=query.split()
        join=' JOIN note_fts f ON f.id=n.id';where.append('note_fts MATCH ?')
        params.append(' AND '.join('"'+t.replace('"','""')+'"' for t in terms))
    if data.get('cursor') is not None:
        c=s._cursor_decode(data['cursor'])
        if set(c)!={'namespace','generation','scope','time','id'} or c['namespace']!=ns or c['generation']!=gen or c['scope']!=scope:
            raise ValueError('stale note page')
        where.append('(n.updated_utc,n.id)>(?,?)');params.extend([utc(c['time']),s.identifier(c['id'])])
    rows=db.execute('SELECT n.* FROM notes n'+join+' WHERE '+' AND '.join(where)+' ORDER BY n.updated_utc,n.id LIMIT ?',params+[limit+1]).fetchall()
    out={'status':'ok','records':[],'more':False,'cursor':None,'data_only':True}
    for i,r in enumerate(rows[:limit]):
        c=s._cursor_encode({'namespace':ns,'generation':gen,'scope':scope,'time':r['updated_utc'],'id':r['id']})
        candidate=out|{'records':out['records']+[summary(r,db)],'more':i+1<len(rows),'cursor':c}
        if not fits(candidate):
            if not out['records']:raise ContextOverflow()
            out['more']=True;break
        out=candidate
    if not rows:out['status']='no_relevant_evidence'
    return bounded_result(out)


def semantic(db,data,embed,themes=False):
    if set(data)-{'query','tag','start','end','limit'}:raise ValueError('unsupported semantic scope')
    query=text(data.get('query'),None);limit=data.get('limit',5)
    if type(limit) is not int or not 1<=limit<=8:raise ValueError('semantic limit must be 1..8')
    where,params=filters(data)
    # Fetch at most cap+1 canonical IDs before loading vectors; never silently select newest notes.
    tables='notes n'
    if data.get('tag') is not None:
        # Force the narrow tag index to drive PK rehydration; unrelated corpus size
        # must not turn a two-note scope into a full status scan/sort.
        tables='note_tags t CROSS JOIN notes n INDEXED BY sqlite_autoindex_notes_1 ON n.id=t.id'
        where=[w for w in where if not w.startswith('n.id IN')];where.insert(0,'t.tag=?')
    rows=db.execute('SELECT n.* FROM '+tables+' WHERE '+' AND '.join(where)+' ORDER BY n.id LIMIT ?',params+[MAX_CANDIDATES+1]).fetchall()
    if len(rows)>MAX_CANDIDATES:
        return {'status':'scope_too_large','hold':True,'records':[],'candidate_cap':MAX_CANDIDATES,'data_only':True}
    if not rows:return {'status':'no_relevant_evidence','records':[],'data_only':True}
    try:
        if embed is None:raise OSError('not configured')
        model,values=embed('query',[query]);q=struct.unpack('<384f',vector(values[0]))
    except (OSError,ValueError,TimeoutError):
        return {'status':'unavailable','records':[],'data_only':True}
    candidates=[];omitted=0;deadline=time.monotonic()+1
    for r in rows:
        if time.monotonic()>deadline:raise ContextOverflow('source_query_overflow')
        v=db.execute('SELECT * FROM note_vectors WHERE id=?',(r['id'],)).fetchone()
        if (v is None or r['semantic_state']!='completed' or v['revision']!=r['revision']
                or v['digest']!=r['original_digest'] or v['model']!=model):
            omitted+=1;continue
        values=struct.unpack('<384f',v['vector'])
        if not all(math.isfinite(x) for x in values):raise ValueError('corrupt note vector')
        score=sum(a*b for a,b in zip(q,values))
        # E5 similarities are ranking signals, not calibrated probabilities or proof of relevance.
        candidates.append((score,r))
    candidates.sort(key=lambda item:(-item[0],item[1]['id']))
    out={'status':'partial' if omitted else 'ok','records':[],'omitted_unindexed_or_stale':omitted,
         'rank_is_inferred':True,'relevance_requires_source_review':True,'data_only':True}
    for score,r in candidates[:limit]:
        candidate=out|{'records':out['records']+[summary(r,db)|{'similarity':round(score,6)}]}
        if not fits(candidate):out['status']='partial';break
        out=candidate
    if not out['records']:out['status']='no_valid_index_candidates' if omitted else 'no_relevant_evidence'
    if themes:
        # Distinct incoming messages and distinct canonical curated meaning prevent retry/chunk duplicates.
        distinct=[];wires=set()
        for record in out['records']:
            if record['origin_source_wire_id'] in wires:continue
            wires.add(record['origin_source_wire_id']);distinct.append(record)
        out['records']=distinct
        out['theme_status']='candidate_related_thoughts' if len(distinct)>=2 else 'insufficient_distinct_support'
        out['inferred_theme']=True
        out['not_a_confirmed_pattern']=True
    return bounded_result(out)


def words(value):
    return set(re.findall(r"\w+",text(value,None).casefold()))


def rule_source(record):
    kind='task' if 'task_id' in record else 'note'
    return kind+':'+s.identifier(record[kind+'_id']),record[kind+'_revision']


def rule_row(db,ref):
    return s._row(db,*ref.split(':',1))


def rule_relevant(r,ref):
    return not r['archived'] and r['status']==('open' if ref.startswith('task:') else 'active')


def rule_summary(db,r,ref):
    if ref.startswith('note:'):return summary(r,db)
    record=json.loads(r['original_json'])
    return bounded_result({'ref':ref,'revision':r['revision'],'digest':r['original_digest'],
                          'title':record['title'],'source':record['source'],'status':r['status']})


def rule_put(db,data):
    if set(data)!={'id','expected_revision','record'} or data['expected_revision']!=0 or type(data['expected_revision']) is not int:
        raise ValueError('new explicit finite message rule required')
    key=s.identifier(data['id']);record=data['record']
    kind='task' if isinstance(record,dict) and 'task_id' in record else 'note'
    fields={kind+'_id',kind+'_revision','purpose','match_terms','count','until_utc','min_seconds','permission_source','permission_wire_id','permission_kind'}
    if not isinstance(record,dict) or set(record)!=fields:raise ValueError('complete explicit message permission required')
    ref,revision=rule_source(record);r=rule_row(db,ref)
    if not rule_relevant(r,ref) or type(revision) is not int or revision!=r['revision']:
        raise ValueError('current active note required')
    text(record['purpose'],None);text(record['permission_source'],None);text(record['permission_wire_id'],128)
    if type(record['count']) is not int or not 1<=record['count']<=7:raise ValueError('finite positive count 1..7 required')
    if type(record['min_seconds']) is not int or not 60<=record['min_seconds']<=604800:raise ValueError('finite spacing required')
    until=utc(record['until_utc'])
    if until<=s.utc_now():raise ValueError('future finite deadline required')
    if record['permission_kind'] not in ('delegated','owner_explicit'):raise ValueError('explicit numeric permission origin required')
    existing=db.execute('SELECT * FROM return_decisions WHERE episode_key=?',(s.digest(record['purpose'].strip().casefold()),)).fetchone()
    if existing is not None and (existing['source_ref']!=ref or existing['decision']!='planned_message' or existing['mechanism_id']!=key):
        raise ValueError('same-purpose selected mechanism cannot be replaced by another permission label')
    if kind=='task' and db.execute('SELECT 1 FROM task_trigger_reservations WHERE task_id=?',(r['id'],)).fetchone():
        raise ValueError('existing task trigger episode cannot acquire another message mechanism')
    if record['permission_kind']=='delegated':
        import return_tools
        p=return_tools.policy(db)
        plan=db.execute('SELECT * FROM return_decisions WHERE source_ref=?',(ref,)).fetchone()
        if (p is None or plan is None or plan['decision']!='planned_message' or plan['mechanism_id']!=key
                or plan['purpose']!=record['purpose'] or plan['source_revision']!=r['revision']
                or record['permission_source'] not in p['authority_refs'] or record['count']!=1
                or record['min_seconds']<p['min_spacing_seconds'] or until>plan['deadline']):
            raise ValueError('standing message return requires exact finite episode plan')
    terms=record['match_terms']
    if (not isinstance(terms,list) or not 1<=len(terms)<=4 or len(set(terms))!=len(terms)
            or any(not isinstance(v,str) or not re.fullmatch(r"\w{1,32}",v) or v!=v.casefold() for v in terms)):
        raise ValueError('1..4 exact normalized match words required')
    db.execute('INSERT INTO note_rules VALUES (?,?,?,1,NULL,1,?,?,?,?,?)',
      (key,s.digest(record['purpose'].strip().casefold()),ref,record['count'],until,record['min_seconds'],None,compact_json(record)))
    db.executemany('INSERT INTO note_rule_terms VALUES (?,?)',[(v,key) for v in terms]);bump(db)
    return {'status':'source_committed','rule_id':key,'revision':1,'remaining':record['count'],'trigger':'actual_incoming_message',
            'timer_created':False,'data_only':True}


def eligible_rule(db,r,tokens,wire,now):
    record=json.loads(r['original_json']);ref,revision=rule_source(record);note=rule_row(db,ref)
    if (not r['active'] or r['remaining']<=0 or r['deadline']<=now or not rule_relevant(note,ref)
            or note['revision']!=revision or not set(record['match_terms'])<=tokens):return False
    if r['last_at'] is not None and (datetime.fromisoformat(now[:-1]+'+00:00')-datetime.fromisoformat(r['last_at'][:-1]+'+00:00')).total_seconds()<r['min_seconds']:
        return False
    return not db.execute('SELECT 1 FROM note_rule_outcomes WHERE rule_id=? AND wire_id=?',(r['id'],wire)).fetchone()


def matches(db,data):
    if set(data)!={'incoming_wire_id','incoming_text'}:raise ValueError('actual incoming trigger required')
    wire=text(data['incoming_wire_id'],128);tokens=words(data['incoming_text'])
    if not tokens:return {'status':'no_matching_rule','records':[],'data_only':True}
    now=s.utc_now()
    ids=db.execute("SELECT DISTINCT t.rule_id FROM note_rule_terms t JOIN note_rules r ON r.id=t.rule_id LEFT JOIN notes n ON n.id=substr(r.source_ref,6) AND substr(r.source_ref,1,5)='note:' LEFT JOIN tasks q ON q.id=substr(r.source_ref,6) AND substr(r.source_ref,1,5)='task:' WHERE t.term IN (SELECT value FROM json_each(?)) AND r.active=1 AND r.remaining>0 AND r.deadline>? AND ((n.status='active' AND n.archived=0 AND n.revision=json_extract(r.original_json,'$.note_revision')) OR (q.status='open' AND q.archived=0 AND q.revision=json_extract(r.original_json,'$.task_revision')))",[compact_json(sorted(tokens)),now]).fetchall()
    out={'status':'no_matching_rule','records':[],'data_only':True};now=s.utc_now()
    for key in ids:
        r=db.execute('SELECT * FROM note_rules WHERE id=?',(key[0],)).fetchone()
        if not eligible_rule(db,r,tokens,wire,now):continue
        record=json.loads(r['original_json']);ref,_=rule_source(record);note=rule_row(db,ref)
        item={'rule_id':r['id'],'rule_revision':r['revision'],'remaining':r['remaining'],'purpose':record['purpose'],
              'until_utc':r['deadline'],'note' if ref.startswith('note:') else 'task':rule_summary(db,note,ref)}
        candidate=out|{'status':'ok','records':out['records']+[item]}
        if not fits(candidate):raise ContextOverflow()
        out=candidate
    return out


def rule_use(db,data):
    kind='task' if 'task_revision' in data else 'note'
    fields={'id','expected_revision','incoming_wire_id','incoming_text',kind+'_revision',kind+'_digest'}
    if set(data)!=fields:raise ValueError('exact current trigger/source reservation required')
    key=s.identifier(data['id']);wire=text(data['incoming_wire_id'],128)
    r=db.execute('SELECT * FROM note_rules WHERE id=?',(key,)).fetchone()
    if r is None:raise ValueError('unknown message rule')
    if db.execute('SELECT 1 FROM note_rule_outcomes WHERE rule_id=? AND wire_id=?',(key,wire)).fetchone():
        return {'status':'already_reserved','rule_id':key,'repeat_response_allowed':False,'data_only':True}
    now=s.utc_now();note=rule_row(db,r['source_ref'])
    if not r['source_ref'].startswith(kind+':'):raise ValueError('exact rule source kind required')
    if (type(data['expected_revision']) is not int or r['revision']!=data['expected_revision']
            or type(data[kind+'_revision']) is not int or note['revision']!=data[kind+'_revision']
            or note['original_digest']!=data[kind+'_digest'] or not eligible_rule(db,r,words(data['incoming_text']),wire,now)):
        raise ValueError('stale or nonmatching message rule')
    purpose=s.digest(json.loads(r['original_json'])['purpose'])
    db.execute('INSERT INTO note_rule_outcomes VALUES (?,?,?,?,?,?)',(key,wire,note['revision'],note['original_digest'],now,purpose))
    db.execute('UPDATE note_rules SET remaining=remaining-1,revision=revision+1,last_at=?,active=CASE WHEN remaining=1 THEN 0 ELSE active END WHERE id=?',(now,key))
    if r['remaining']==1:db.execute('DELETE FROM note_rule_terms WHERE rule_id=?',(key,))
    bump(db)
    return {'status':'response_slot_reserved','rule_id':key,'revision':r['revision']+1,'remaining':r['remaining']-1,
            'delivery_unconfirmed':True,'timer_created':False,'data_only':True}


def rule_rebind(db,data):
    kind='task' if 'task_revision' in data else 'note'
    if set(data)!={'id','expected_revision',kind+'_revision','rationale'}:raise ValueError('exact correction continuation required')
    r=db.execute('SELECT * FROM note_rules WHERE id=?',(s.identifier(data['id']),)).fetchone()
    if r is None or type(data['expected_revision']) is not int or r['revision']!=data['expected_revision']:
        raise ValueError('rule revision conflict')
    note=rule_row(db,r['source_ref'])
    if (not r['source_ref'].startswith(kind+':') or r['invalidated_by']!=kind+'_put' or r['remaining']<=0 or r['deadline']<=s.utc_now() or not rule_relevant(note,r['source_ref'])
            or type(data[kind+'_revision']) is not int or note['revision']!=data[kind+'_revision']):
        raise ValueError('only unconsumed same-purpose correction can continue')
    record=json.loads(r['original_json']);record[kind+'_revision']=note['revision']
    record['correction_rationale']=text(data['rationale'],None)
    db.execute('UPDATE note_rules SET revision=revision+1,active=1,invalidated_by=NULL,original_json=? WHERE id=?',(compact_json(record),r['id']))
    db.executemany('INSERT OR IGNORE INTO note_rule_terms VALUES (?,?)',[(term,r['id']) for term in record['match_terms']])
    bump(db)
    return {'status':'source_committed','rule_id':r['id'],'revision':r['revision']+1,'remaining':r['remaining'],
            'budget_reset':False,'data_only':True}


def run(db,action,data,embed=None):
    check(db)
    if action=='note_get':
        if set(data)!={'id'}:raise ValueError('exact note ID required')
        return snapshot(db,'note:'+s.identifier(data['id']))
    if action in ('note_put','note_forget','note_archive','note_restore','note_resolve'):return put(db,action,data)
    if action=='note_index':return index(db,data,embed)
    if action in ('note_list','note_search'):return page(db,data,action=='note_search')
    if action in ('note_semantic','note_themes'):return semantic(db,data,embed,action=='note_themes')
    if action=='note_rule_put':return rule_put(db,data)
    if action=='note_match':return matches(db,data)
    if action=='note_rule_use':return rule_use(db,data)
    if action=='note_rule_rebind':return rule_rebind(db,data)
    if action=='note_rule_cancel':
        if set(data)!={'id','expected_revision'} or type(data['expected_revision']) is not int:raise ValueError('rule CAS required')
        r=db.execute('SELECT * FROM note_rules WHERE id=?',(s.identifier(data['id']),)).fetchone()
        if r is None or r['revision']!=data['expected_revision']:raise ValueError('rule revision conflict')
        db.execute('DELETE FROM note_rule_terms WHERE rule_id=?',(r['id'],))
        db.execute("UPDATE note_rules SET active=0,revision=revision+1,invalidated_by='cancelled' WHERE id=?",(r['id'],));bump(db)
        return {'status':'source_committed','rule_id':r['id'],'revision':r['revision']+1,'active':False,'data_only':True}
    raise ValueError('unknown note action')
