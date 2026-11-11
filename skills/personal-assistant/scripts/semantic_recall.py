#!/usr/bin/env python3
"""One bounded technical-note recall. No personal payload, retention or authority."""
import hashlib
from datetime import date
import json
import re
import socket
import time
import urllib.request

import context_budget as budget
import source_store as source
from legacy_source import pairs

ENDPOINT = 'http://127.0.0.1:9077/v1/default/banks/codex-safe-notes/memories/recall'
TOPICS = {
    'storage-design': 'Personal assistant indexed source storage revision migration technical design',
    'reminder-runtime': 'Personal assistant reminder runtime single sender uncertainty dedup technical design',
    'context-budgets': 'Personal assistant bounded recovery output envelope token budget technical design',
}
SCOPE = re.compile(r'project:[0-9a-f]{16}\Z')
SECRET = re.compile(r'(?i)(-----BEGIN .*PRIVATE KEY|\bBearer\s+\S+|\b(?:sk-|xox[bp]-|gh[pou]_|github_pat_)\S+|\b(?:password|api[_ -]?key|access[_ -]?token|secret)\s*[:=]\s*\S+|\b\d{5,}:[A-Za-z0-9_-]{20,})')


def text_digest(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def configure_scope(db, scope):
    """Explicit operator setup only. Nonempty project scope; never a model arg."""
    if not isinstance(scope, str) or not SCOPE.fullmatch(scope):
        raise ValueError('strict technical-note project scope required')
    with source.atomic(db, True):
        old = db.execute("SELECT value FROM metadata WHERE key='semantic_scope'").fetchone()
        if old is not None and old[0] != scope:
            raise ValueError('semantic scope change requires reviewed pointer migration')
        db.execute("INSERT OR IGNORE INTO metadata VALUES ('semantic_scope',?)", (scope,))


def register_note(db, note_id, scope, text, expected_revision):
    """Operator-curated SAFE TECHNICAL notes only, no shared-bank retention.

    Secret matching is defense in depth; caller reviews absence of personal
    data and permission. This API is absent from all model-visible commands.
    """
    source.identifier(note_id)
    if (not isinstance(text, str) or not text.strip() or '\x00' in text
            or SECRET.search(text) or type(expected_revision) is not int or expected_revision < 0
            or scope != source.meta(db, 'semantic_scope')):
        raise ValueError('invalid curated technical note')
    with source.atomic(db, True):
        old=db.execute('SELECT revision FROM curated_notes WHERE id=?', (note_id,)).fetchone()
        if (old is None and expected_revision != 0) or (old is not None and old[0] != expected_revision):
            raise ValueError('curated note revision conflict')
        db.execute('''INSERT INTO curated_notes VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
          scope=excluded.scope,revision=excluded.revision,text=excluded.text,digest=excluded.digest''',
                   (note_id,scope,expected_revision+1,text,text_digest(text)))


def register_pointer(db, fact_id, note_id, fact_text, as_of):
    """Operator verifies exact extracted fact and its safe local note anchor."""
    if (not isinstance(fact_id,str) or not 1<=len(fact_id)<=80 or '\x00' in fact_id
            or not isinstance(fact_text,str) or not fact_text.strip() or SECRET.search(fact_text)
            or not isinstance(as_of,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',as_of)):
        raise ValueError('invalid verified semantic pointer')
    date.fromisoformat(as_of)
    with source.atomic(db,True):
        note=db.execute('SELECT * FROM curated_notes WHERE id=?',(note_id,)).fetchone()
        if note is None or note['scope']!=source.meta(db,'semantic_scope'):
            raise ValueError('safe local note missing')
        db.execute('INSERT INTO semantic_pointers VALUES (?,?,?,?,?,?,?)',
                   (fact_id,note['scope'],'technical-note:'+note_id,note['revision'],note['digest'],as_of,text_digest(fact_text)))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):
        raise ValueError('semantic redirect refused')


def fetch_bounded(body):
    """Reviewed loopback bank only. Complete response within a finite deadline."""
    deadline=time.monotonic()+5
    request=urllib.request.Request(ENDPOINT,data=budget.compact_json(body).encode(),
                                    headers={'Content-Type':'application/json','Accept-Encoding':'identity'})
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    with opener.open(request,timeout=5) as reply:
        if reply.headers.get('Content-Encoding','identity') != 'identity':
            raise ValueError('semantic compression refused')
        declared=reply.headers.get('Content-Length')
        if declared is not None and not declared.isdigit():
            raise ValueError('invalid semantic content length')
        # Supported CPython HTTPResponse socket: update each read timeout to
        # remaining overall deadline, including connection/header time.
        sock=getattr(getattr(reply.fp,'raw',None),'_sock',None)
        if sock is None:
            raise ValueError('deadline socket unavailable')
        chunks=[]
        while True:
            if reply.isclosed():break
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError()
            sock.settimeout(remaining)
            chunk=reply.read1(8192)
            if time.monotonic()>deadline:raise TimeoutError()
            if not chunk:break
            chunks.append(chunk)
    return json.loads(b''.join(chunks),object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite recall JSON')))


def envelope(status,scope=None):
    return {'status':status,'data_only':True,'authority':'historical_candidates_only',
            'scope':scope,'facts':[],'returned':0,'omitted':0,'more':False,
            'omission_counts':{k:0 for k in ('invalid','scope','length','stale','duplicate','budget')}}


def project(db,raw,scope):
    rows=raw.get('results') if isinstance(raw,dict) else None
    if not isinstance(rows,list):
        return budget.bounded_result(envelope('invalid_candidates',scope),'memory')
    result=envelope('ok',scope);seen=set()
    def omit(reason):
        result['omitted']+=1;result['omission_counts'][reason]+=1
    with source.atomic(db),source.bounded_work(db):
        for row in rows:
            source.check_work(db)
            if not isinstance(row,dict):omit('invalid');continue
            tags=row.get('tags')
            if not isinstance(tags,list) or not {'codex-safe-note',scope}.issubset({x for x in tags if isinstance(x,str)}):
                omit('scope');continue
            fact_id,text=row.get('id'),row.get('text')
            if not isinstance(fact_id,str) or not 1<=len(fact_id)<=80 or not isinstance(text,str) or not text or '\x00' in text:
                omit('invalid');continue
            pointer=db.execute('SELECT * FROM semantic_pointers WHERE fact_id=?',(fact_id,)).fetchone()
            if pointer is None or pointer['scope']!=scope or pointer['fact_digest']!=text_digest(text):
                omit('stale');continue
            ref=pointer['source_ref']
            note=db.execute('SELECT * FROM curated_notes WHERE id=?',(ref.removeprefix('technical-note:'),)).fetchone()
            if (not ref.startswith('technical-note:') or note is None or note['scope']!=scope
                    or note['revision']!=pointer['source_revision'] or note['digest']!=pointer['source_digest']):
                omit('stale');continue
            dedup=(ref,' '.join(text.casefold().split()))
            if dedup in seen:omit('duplicate');continue
            seen.add(dedup)
            fact={k:pointer[k] for k in ('source_ref','source_revision','source_digest','as_of')}
            fact.update(id=fact_id,text=text)
            result['facts'].append(fact);result['returned']+=1
    if result['omitted']:result['status']='partial' if result['returned'] else 'no_valid_candidates'
    return budget.bounded_result(result,'memory')


def recall(db,topic,fetch=None):
    if topic not in TOPICS:raise ValueError('safe technical topic required')
    try:scope=source.meta(db,'semantic_scope')
    except ValueError:return envelope('not_configured')
    if not SCOPE.fullmatch(scope):return envelope('not_configured')
    request={'query':TOPICS[topic],'budget':'low','tags':['codex-safe-note',scope],
             'tags_match':'all_strict','include':{'entities':None,'chunks':None,'source_facts':None},'trace':False}
    try:return project(db,(fetch or fetch_bounded)(request),scope)
    except budget.ContextOverflow:return envelope('response_too_large',scope)
    except (OSError,ValueError,TypeError,socket.timeout):return envelope('unavailable',scope)
