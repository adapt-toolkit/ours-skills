"""Targeted existing task-message intent recovery and one trigger-relative repeat.

The trusted assistant validates the authenticated Owner envelope and semantics.
CLI strings/boolean attestations are provenance, never an authentication service.
No task import, guessed greeting clock, broad scan, sender or extra timer loop.
"""
import json
from datetime import datetime,time,timedelta,timezone
from zoneinfo import ZoneInfo
from pathlib import Path
import sqlite3

import source_store as s
import preference_tools as preferences
from context_budget import bounded_result

SCHEMA='''
CREATE TABLE task_trigger_reservations(
 task_id TEXT PRIMARY KEY,source_revision INTEGER NOT NULL,source_digest TEXT NOT NULL,
 incoming_wire_id TEXT NOT NULL,incoming_utc TEXT NOT NULL,permission_wire_id TEXT NOT NULL,
 operation_id TEXT NOT NULL UNIQUE,request_json TEXT,status TEXT NOT NULL CHECK(status IN ('prepared','timer_committed','suppressed')),
 receipt_json TEXT);
'''
FIELDS=('intended_local_date','reminder_trigger','additional_reminder_count','additional_reminder_delay_seconds','repeat_condition')


def fields(row):
    original=json.loads(row['original_json']);values={k:original[k] for k in FIELDS if k in original}
    if set(values)!=set(FIELDS):raise ValueError('complete selected saved trigger intent required')
    if (not isinstance(values['intended_local_date'],str) or len(values['intended_local_date'])!=10
            or datetime.strptime(values['intended_local_date'],'%Y-%m-%d').date().isoformat()!=values['intended_local_date']):
        raise ValueError('exact saved local date required')
    for key in ('reminder_trigger','repeat_condition'):
        value=values[key]
        if not isinstance(value,str) or not value.strip() or '\x00' in value:raise ValueError('complete saved symbolic semantics required')
    if type(values['additional_reminder_count']) is not int or type(values['additional_reminder_delay_seconds']) is not int:
        raise ValueError('typed explicit per-intent numeric values required')
    return values


def get(db,data):
    if set(data)-{'id','provenance_key'} or 'id' not in data:raise ValueError('one exact task trigger required')
    row=s._row(db,'task',data['id']);values=fields(row)
    result={'ref':'task:'+row['id'],'revision':row['revision'],'digest':row['original_digest'],
            'status':row['status'],'archived':bool(row['archived']),'intent':values,
            'timezone':preferences.read(db,['timezone']),'data_only':True,
            'semantics_require_authenticated_owner_review':True}
    # Names/types only help identify the one preserved confirmation field.
    original=json.loads(row['original_json'])
    extra=[k for k in original if k not in FIELDS and k not in ('title','source','status','task_schema_version','id','prerequisites')]
    result['extension_types']={k:type(original[k]).__name__ for k in extra}
    if 'provenance_key' in data:
        key=data['provenance_key']
        if key not in extra:raise ValueError('one exact preserved provenance key required')
        value=original[key]
        result['selected_provenance']={key:value}
    existing=db.execute('SELECT * FROM task_trigger_reservations WHERE task_id=?',(row['id'],)).fetchone()
    result['reservation']=None if existing is None else {'status':existing['status'],'operation_id':existing['operation_id'],
      'initial_response_allowed':False,'incoming_wire_id':existing['incoming_wire_id'],'delivery_unconfirmed':True}
    return bounded_result(result)


def reserve(db,data):
    import note_store as n
    required={'id','expected_revision','digest','incoming_wire_id','incoming_utc','permission_wire_id','match_confirmed','permission_confirmed'}
    if set(data)!=required or data['match_confirmed'] is not True or data['permission_confirmed'] is not True:
        raise ValueError('actual authenticated trigger and repeat permission review required')
    row=s._row(db,'task',data['id']);old=db.execute('SELECT * FROM task_trigger_reservations WHERE task_id=?',(row['id'],)).fetchone()
    if old is not None:
        return {'status':'already_reserved','operation_id':old['operation_id'],'initial_response_allowed':False,
                'same_trigger':old['incoming_wire_id']==data['incoming_wire_id'],'data_only':True}
    if type(data['expected_revision']) is not int or row['revision']!=data['expected_revision'] or row['original_digest']!=data['digest']:
        raise ValueError('current exact saved trigger source required')
    plan=db.execute('SELECT decision FROM return_decisions WHERE source_ref=?',('task:'+row['id'],)).fetchone()
    if (plan is not None and plan['decision'] not in ('no_return','unresolved_context')) or db.execute(
        'SELECT 1 FROM note_rules WHERE source_ref=?',('task:'+row['id'],)).fetchone():
        raise ValueError('existing task episode mechanism must be reconciled, not duplicated')
    values=fields(row);zone=preferences.read(db,['timezone'])
    if zone.get('confirmed') is not True:raise ValueError('confirmed timezone provenance required')
    tz=ZoneInfo(zone['timezone']);wire=n.text(data['incoming_wire_id'],128);permission=n.text(data['permission_wire_id'],128)
    instant=datetime.fromisoformat(n.utc(data['incoming_utc'])[:-1]+'+00:00');now=datetime.now(timezone.utc)
    day=instant.astimezone(tz).date()
    if instant>now+timedelta(seconds=5) or day.isoformat()!=values['intended_local_date']:
        raise ValueError('actual incoming occurrence must match saved local day')
    # Supported legacy repeat contract is deliberately exact, not an inferred rule.
    if values['additional_reminder_count']!=1 or values['additional_reminder_delay_seconds']!=3600:
        raise ValueError('unsupported saved finite repeat contract; preserve and hold')
    end=datetime.combine(day+timedelta(days=1),time.min,tzinfo=tz).astimezone(timezone.utc)
    due=instant+timedelta(seconds=values['additional_reminder_delay_seconds']);expires=min(due+timedelta(hours=1),end)
    op='task-trigger-'+s.digest([row['id'],values['intended_local_date'],wire])[:40]
    status='prepared';request=None
    eligible=row['status']=='open' and not row['archived'] and now<end
    if not eligible or due<=now or (expires-due).total_seconds()<1:status='suppressed'
    else:
        local=due.astimezone(tz)
        original=json.loads(row['original_json'])
        request={'operation_id':op,'expected_source_revision':row['revision'],'source_ref':'task:'+row['id'],
                 'title':original['title'],'context':'Review the still-open task after its actual agreed message trigger.',
                 'mode':'contextual','local_time':local.replace(tzinfo=None).isoformat(),'timezone':zone['timezone'],
                 'fold':local.fold,'late_window_seconds':(expires-due).total_seconds(),'followup_limit':0,
                 'owner_request_wire_id':permission}
    db.execute('INSERT INTO task_trigger_reservations VALUES (?,?,?,?,?,?,?,?,?,NULL)',
      (row['id'],row['revision'],row['original_digest'],wire,data['incoming_utc'],permission,op,
       None if request is None else s.compact_json(request),status));n.bump(db)
    return {'status':'response_slot_reserved' if eligible else 'suppressed','operation_id':op,
            'initial_response_allowed':eligible,'repeat_state':status,'delivery_unconfirmed':True,'data_only':True}


def finish(db,cfg,data):
    import reminder_tools as runtime
    import return_tools
    import note_store as n
    if set(data)!={'id','incoming_wire_id'}:raise ValueError('exact reserved trigger finish required')
    saved=db.execute('SELECT * FROM task_trigger_reservations WHERE task_id=?',(s.identifier(data['id']),)).fetchone()
    if saved is None or saved['incoming_wire_id']!=data['incoming_wire_id']:raise ValueError('exact winning trigger required')
    if saved['status']!='prepared':return {'status':saved['status'],'initial_response_allowed':False,'receipt_is_historical':True,'data_only':True}
    row=s._row(db,'task',saved['task_id'])
    if row['revision']!=saved['source_revision'] or row['original_digest']!=saved['source_digest'] or row['status']!='open' or row['archived']:
        return {'status':'held_source_changed','initial_response_allowed':False,'operation_id':saved['operation_id'],'data_only':True}
    try:
        timer=json.loads(saved['request_json']);store=runtime.Store(Path(cfg['database']))
        try:receipt=runtime.mutate_with_source_journal(store,cfg,'create',timer)
        finally:store.db.close()
        proof=return_tools.verifier(cfg)(saved['operation_id'],receipt['id'],'task:'+saved['task_id'],saved['source_revision'])
        with s.atomic(db,True):
            db.execute("UPDATE task_trigger_reservations SET status='timer_committed',receipt_json=? WHERE task_id=? AND status='prepared'",
                       (s.compact_json(proof),saved['task_id']));n.bump(db)
        return {'status':'timer_committed','operation_id':saved['operation_id'],'reminder_id':receipt['id'],
                'delivery_unconfirmed':True,'data_only':True}
    except (ValueError,OSError,KeyError,TypeError,sqlite3.Error):
        return {'status':'held_reconciliation_required','operation_id':saved['operation_id'],'return_committed':False,
                'reservation_committed':True,'mechanism_state':'unverified','delivery_unconfirmed':True,'data_only':True}
