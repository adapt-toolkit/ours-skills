#!/usr/bin/env python3
"""Finite delegated return decisions; exact commit evidence, never schedules or sends."""
import json
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta
import time

import source_store as s
from context_budget import bounded_result

POLICY = {'version':1,'numeric_origin':'assistant_default','initial_returns_per_purpose':1,'followup_limit':0,
          'max_horizon_seconds':604800,'default_late_window_seconds':3600,'min_spacing_seconds':3600,
          'known_timed_opportunity_required':True,'one_mechanism_per_purpose':True}
SCHEMA='''
CREATE TABLE return_decisions(
 source_ref TEXT PRIMARY KEY,episode_key TEXT NOT NULL UNIQUE,source_revision INTEGER NOT NULL,revision INTEGER NOT NULL,
 decision TEXT NOT NULL CHECK(decision IN ('no_return','unresolved_context','planned_timed','planned_message','committed_timed','committed_message')),
 policy_json TEXT NOT NULL,reason TEXT NOT NULL,purpose TEXT NOT NULL,authority_ref TEXT NOT NULL,
 intake_utc TEXT NOT NULL,deadline TEXT,mechanism_id TEXT,receipt_json TEXT);
'''


def configure(db,authority_refs):
    """Private reviewed operator binding to standing Owner delegation; no inferred authority."""
    import note_store as n
    if not isinstance(authority_refs,list) or not 1<=len(authority_refs)<=4:raise ValueError('verified delegation refs required')
    for ref in authority_refs:n.text(ref,256)
    policy=POLICY|{'authority_kind':'owner_delegated_contextual_timing','authority_refs':authority_refs}
    with s.atomic(db,True):
        old=db.execute("SELECT value FROM metadata WHERE key='return_policy'").fetchone()
        value=s.compact_json(policy)
        if old is not None and old[0]!=value:raise ValueError('standing policy replacement requires reviewed migration')
        db.execute("INSERT OR IGNORE INTO metadata VALUES ('return_policy',?)",(value,))
    return {'policy_version':1,'numeric_origin':'assistant_default','data_only':True}


def policy(db):
    try:r=json.loads(s.meta(db,'return_policy'))
    except ValueError:return None
    if any(r.get(k)!=v for k,v in POLICY.items()) or r.get('authority_kind')!='owner_delegated_contextual_timing':
        raise ValueError('unknown standing return policy')
    return r


def current(db,ref,revision):
    if not isinstance(ref,str) or ':' not in ref:raise ValueError('exact return source required')
    kind,key=ref.split(':',1)
    if kind not in ('task','note'):raise ValueError('note/task return required')
    r=s._row(db,kind,key)
    if type(revision) is not int or r['revision']!=revision:raise ValueError('return source revision conflict')
    return r


def plan(db,data):
    import note_store as n
    fields={'ref','expected_source_revision','expected_revision','decision','reason','purpose','authority_ref'}
    if set(data)-fields-{'deadline','mechanism_id'} or not fields<=set(data):raise ValueError('complete intake decision required')
    ref=data['ref'];r=current(db,ref,data['expected_source_revision']);old=db.execute('SELECT * FROM return_decisions WHERE source_ref=?',(ref,)).fetchone()
    expected=data['expected_revision']
    if type(expected) is not int or expected<0 or (old is None and expected!=0) or (old is not None and old['revision']!=expected):
        raise ValueError('return decision conflict')
    if old is not None and old['decision'].startswith('committed_'):
        raise ValueError('existing purpose already has a return; preserve/adjust its exact mechanism, never reset budget')
    if old is not None and old['purpose']!=data['purpose']:raise ValueError('preserve original purpose episode')
    # Preparation is durable before the caller receives/records commit evidence.
    # Keep that crash window frozen, including cancelled/consumed rule history.
    purpose_key=s.digest(n.text(data['purpose'],None).strip().casefold())
    rule=db.execute('SELECT 1 FROM note_rules WHERE purpose_key=?',(purpose_key,)).fetchone()
    prepared=None if old is None or old['mechanism_id'] is None else db.execute(
        'SELECT 1 FROM source_schedule_operations WHERE operation_id=?',(old['mechanism_id'],)).fetchone()
    trigger=None if not ref.startswith('task:') else db.execute('SELECT 1 FROM task_trigger_reservations WHERE task_id=?',(ref.split(':',1)[1],)).fetchone()
    if trigger:raise ValueError('existing explicit trigger episode cannot acquire a discretionary mechanism')
    if rule or prepared:raise ValueError('materialized purpose mechanism is frozen; reconcile its exact identity')
    decision=data['decision']
    if decision not in ('no_return','unresolved_context','planned_timed','planned_message'):raise ValueError('intake decision required')
    reason=n.text(data['reason'],None);purpose=n.text(data['purpose'],None);authority=n.text(data['authority_ref'],256)
    p=policy(db);now=s.utc_now();deadline=data.get('deadline');mechanism=data.get('mechanism_id')
    if decision.startswith('planned_'):
        if p is None:raise ValueError('standing return policy not configured')
        if authority not in p['authority_refs']:raise ValueError('unsupported delegated authority reference')
        if r['archived'] or r['status']!=('open' if ref.startswith('task:') else 'active'):raise ValueError('source not relevant')
        deadline=n.utc(deadline);s.identifier(mechanism)
        # Updating an uncommitted plan cannot perpetually renew the original episode horizon.
        origin=old['intake_utc'] if old is not None else now
        horizon=(datetime.fromisoformat(origin[:-1]+'+00:00')+timedelta(seconds=p['max_horizon_seconds'])).isoformat(timespec='microseconds').replace('+00:00','Z')
        if not now<deadline<=horizon:raise ValueError('finite relevance horizon required')
    elif deadline is not None or mechanism is not None:raise ValueError('non-return cannot claim a mechanism')
    db.execute('''INSERT INTO return_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL) ON CONFLICT(source_ref) DO UPDATE SET
      source_revision=excluded.source_revision,revision=excluded.revision,decision=excluded.decision,
      policy_json=excluded.policy_json,reason=excluded.reason,purpose=excluded.purpose,authority_ref=excluded.authority_ref,
      deadline=excluded.deadline,mechanism_id=excluded.mechanism_id,receipt_json=NULL''',
      (ref,s.digest(purpose.strip().casefold()),r['revision'],expected+1,decision,s.compact_json(p or {}),reason,purpose,authority,old['intake_utc'] if old is not None else now,deadline,mechanism))
    n.bump(db)
    return {'status':'source_committed','ref':ref,'decision':decision,'revision':expected+1,'return_committed':False,'data_only':True}


def verifier(cfg):
    def verify(op,rid,ref,source_revision):
        if not cfg.get('source_journal') or not cfg.get('database'):raise ValueError('complete configured return stores required')
        journal=s.private_path(Path(cfg['source_journal']));ledger=s.private_path(Path(cfg['database']))
        j=sqlite3.connect(journal.as_uri()+'?mode=ro',uri=True,timeout=1);l=sqlite3.connect(ledger.as_uri()+'?mode=ro',uri=True,timeout=1)
        j.row_factory=l.row_factory=sqlite3.Row
        try:
            receipt=j.execute('SELECT * FROM operations WHERE operation_id=?',(op,)).fetchone()
            r=l.execute('SELECT * FROM reminders WHERE id=?',(rid,)).fetchone()
            operation=l.execute('SELECT * FROM operation_receipts WHERE operation_id=?',(op,)).fetchone()
            held=l.execute('SELECT 1 FROM reminder_operation_holds WHERE operation_id=?',(op,)).fetchone()
            if receipt is None or receipt['stage']!='complete' or r is None or operation is None or not operation['linked'] or held:
                raise ValueError('timer operation not durably complete')
            request=json.loads(receipt['request_json']);result=json.loads(receipt['result_json'])
            if (request['source_ref']!=ref or request['expected_source_revision']!=source_revision or result['id']!=rid
                    or r['source_ref']!=ref or r['status']!='active' or r['mode']!='contextual' or r['followup_limit']!=0):
                raise ValueError('standing timer receipt not current or finite')
            return {'reminder_id':rid,'reminder_revision':r['revision'],'operation_id':op,'due':r['due'],'expires':r['expires'],
                    'followup_limit':0,'late_window_seconds':r['expires']-r['due'],'state':'scheduled_not_delivered'}
        finally:j.close();l.close()
    return verify


def commit(db,data,verify):
    import note_store as n
    if set(data)-{'ref','expected_revision','expected_source_revision','reminder_id'}:raise ValueError('exact return commit required')
    r=current(db,data['ref'],data['expected_source_revision']);old=db.execute('SELECT * FROM return_decisions WHERE source_ref=?',(data['ref'],)).fetchone()
    if old is None or type(data['expected_revision']) is not int or old['revision']!=data['expected_revision']:
        raise ValueError('return plan conflict')
    if old['source_revision']!=r['revision'] or r['archived'] or r['status']!=('open' if old['source_ref'].startswith('task:') else 'active'):
        raise ValueError('return plan source changed')
    if old['decision'].startswith('committed_'):
        return {'status':'already_committed','ref':old['source_ref'],'decision':old['decision'],'revision':old['revision'],'data_only':True}
    if old['decision']=='planned_timed':
        if verify is None:raise ValueError('timer receipt verifier required')
        mapping=db.execute('SELECT * FROM reminder_mappings WHERE reminder_id=?',(data.get('reminder_id'),)).fetchone()
        if mapping is None or mapping['operation_id']!=old['mechanism_id'] or mapping['source_ref']!=old['source_ref'] or mapping['source_revision']!=r['revision']:
            raise ValueError('missing exact timer source mapping')
        proof=verify(old['mechanism_id'],data['reminder_id'],old['source_ref'],r['revision'])
        deadline=datetime.fromisoformat(old['deadline'][:-1]+'+00:00').timestamp()
        if (proof['expires']>deadline or proof['due']>=deadline or proof['expires']<=time.time()
                or proof['late_window_seconds']>POLICY['default_late_window_seconds']):raise ValueError('timer exceeds finite relevance policy')
        decision='committed_timed'
    elif old['decision']=='planned_message':
        if 'reminder_id' in data:raise ValueError('message trigger is not a timer')
        rule=db.execute('SELECT * FROM note_rules WHERE id=?',(old['mechanism_id'],)).fetchone()
        if (rule is None or not rule['active'] or rule['remaining']!=1 or rule['deadline']<=s.utc_now() or old['source_ref']!=rule['source_ref']
                or rule['deadline']>old['deadline'] or n.rule_source(json.loads(rule['original_json']))[1]!=r['revision']):
            raise ValueError('missing current finite message mechanism')
        proof={'rule_id':rule['id'],'rule_revision':rule['revision'],'remaining':1,'state':'pending_matching_incoming_message'}
        decision='committed_message'
    else:raise ValueError('no proposed mechanism to commit')
    db.execute('UPDATE return_decisions SET decision=?,revision=revision+1,receipt_json=? WHERE source_ref=?',
               (decision,s.compact_json(proof),old['source_ref']));n.bump(db)
    return {'status':'return_committed','ref':old['source_ref'],'decision':decision,'revision':old['revision']+1,
            'delivery_unconfirmed':True,'data_only':True}


def run(db,action,data,verify=None):
    import note_store as n
    n.check(db)
    if action=='return_plan':return bounded_result(plan(db,data))
    if action=='return_commit':return bounded_result(commit(db,data,verify))
    if action=='return_get':
        if set(data)!={'ref'}:raise ValueError('exact return source required')
        r=db.execute('SELECT * FROM return_decisions WHERE source_ref=?',(data['ref'],)).fetchone()
        if r is None:return {'status':'not_considered','data_only':True}
        source=current(db,r['source_ref'],s._row(db,*r['source_ref'].split(':',1))['revision'])
        if r['source_ref'].startswith('note:') and source['status'] in ('forgotten','archived'):
            return {'ref':r['source_ref'],'revision':r['revision'],'content_excluded':True,'source_relevant':False,'data_only':True}
        return bounded_result({'ref':r['source_ref'],'revision':r['revision'],'source_revision':r['source_revision'],
          'source_current':source['revision']==r['source_revision'],'source_relevant':not source['archived'] and source['status'] in ('active','open'),
          'decision':r['decision'],'mechanism_state':'not_revalidated','receipt_is_historical_commit_evidence':True,'delivery_unconfirmed':True,'reason':r['reason'],'purpose':r['purpose'],'receipt':json.loads(r['receipt_json']) if r['receipt_json'] else None,
          'data_only':True})
    raise ValueError('unknown return action')
