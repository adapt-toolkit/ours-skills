"""Bounded adaptive actual-message task trigger; durable shared envelope ticket."""
import json
import secrets
from datetime import datetime,timezone,timedelta
import sqlite3
import fcntl
import os
from pathlib import Path
import stat

import context_budget as b
import source_store as s
import task_triggers as triggers

SCHEMA='''
CREATE TABLE task_trigger_tickets(
 ticket TEXT PRIMARY KEY,incoming_wire_id TEXT NOT NULL UNIQUE,task_id TEXT NOT NULL,
 incoming_utc TEXT NOT NULL,source_revision INTEGER NOT NULL,source_digest TEXT NOT NULL,
 created_utc TEXT NOT NULL,stage TEXT NOT NULL CHECK(stage IN ('open','provenance','pending','complete','held')),
 attempts INTEGER NOT NULL,budget_json TEXT NOT NULL,base_budget_json TEXT,reservation_json TEXT);
'''

class BudgetExhausted(RuntimeError):pass
class SourceChanged(ValueError):pass


def save_budget(db,ticket,budget):
    db.execute('UPDATE task_trigger_tickets SET budget_json=? WHERE ticket=?',(s.compact_json(budget.snapshot()),ticket))


def failure(db,ticket,reason='source_changed'):
    with s.atomic(db,True):
        row=db.execute('SELECT * FROM task_trigger_tickets WHERE ticket=?',(ticket,)).fetchone()
        budget=b.TurnBudget(json.loads(row['budget_json']))
        try:output=budget.admit_failure(reason)
        except b.ContextOverflow:raise BudgetExhausted('no further ticket output') from None
        save_budget(db,ticket,budget)
        db.execute("UPDATE task_trigger_tickets SET stage='held' WHERE ticket=? AND stage NOT IN ('pending','complete')",(ticket,))
        return output


def request_failure(db,request,reason='invalid_request'):
    """Resolve identifiable host ownership before any malformed-request output.

    Only exact indexed handles/wires select existing counters; malformed shape,
    another workflow name or changed bindings cannot mint a fresh error budget.
    Ambiguous ownership fails silently rather than choosing a looser budget.
    """
    if not isinstance(request,dict):return None
    owners=set()
    for mode in ('prepare_task_trigger','continue_task_trigger','prepare_message_return','continue_message_return'):
        data=request.get(mode)
        if not isinstance(data,dict):continue
        for field in ('ticket','incoming_wire_id'):
            value=data.get(field)
            if not isinstance(value,str) or not 0<len(value)<=128:continue
            for table in ('task_trigger_tickets','message_return_tickets'):
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone() is None:continue
                row=db.execute('SELECT ticket FROM '+table+' WHERE '+field+'=?',(value,)).fetchone()
                if row is not None:owners.add((table,row[0]))
    if not owners:return None
    if len(owners)!=1:raise BudgetExhausted('ambiguous existing ticket; no further output')
    table,ticket=owners.pop()
    try:
        if table=='task_trigger_tickets':return failure(db,ticket,reason)
        import message_return_session
        return message_return_session.failure(db,ticket,reason)
    except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
        # No generic fallback if known counters cannot be safely read/charged.
        raise BudgetExhausted('existing ticket cannot authorize further output') from None


def run(db,request,cfg):
    try:return _locked_run(db,request,cfg)
    except (ValueError,OSError,KeyError,TypeError,sqlite3.Error) as error:
        output=request_failure(db,request,str(error) if isinstance(error,b.ContextOverflow) else 'invalid_request')
        if output is not None:return output
        raise


def _locked_run(db,request,cfg):
    # Hold one process lock through cross-store finish; concurrent continuation
    # cannot race a live finisher or refund another response's persistent charge.
    filename=db.execute('PRAGMA database_list').fetchone()[2]
    path=Path(filename).with_suffix('.trigger-session.lock')
    fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_nlink!=1 or stat.S_IMODE(info.st_mode)!=0o600:raise ValueError('private session lock required')
        try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            data=request.get('continue_task_trigger',{})
            if isinstance(data,dict) and isinstance(data.get('ticket'),str) and db.execute(
                'SELECT 1 FROM task_trigger_tickets WHERE ticket=?',(data['ticket'],)).fetchone():
                return failure(db,data['ticket'],'source_unavailable')
            raise ValueError('actual trigger session busy') from None
        return _run(db,request,cfg)
    finally:os.close(fd)


def _run(db,request,cfg):
    import note_store as n
    n.check(db)
    if set(request)=={'prepare_task_trigger'}:
        data=request['prepare_task_trigger']
        if not isinstance(data,dict) or set(data)!={'id','incoming_wire_id','incoming_utc'}:raise ValueError('exact actual trigger prepare required')
        s.identifier(data['id']);n.text(data['incoming_wire_id'],128);n.utc(data['incoming_utc'])
        instant=datetime.fromisoformat(data['incoming_utc'][:-1]+'+00:00')
        if instant>datetime.now(timezone.utc)+timedelta(seconds=5):raise ValueError('actual occurrence required')
        with s.atomic(db,True),s.bounded_work(db):
            generic=db.execute('SELECT ticket FROM message_return_tickets WHERE incoming_wire_id=?',(data['incoming_wire_id'],)).fetchone()
            if generic is not None:
                import message_return_session
                return message_return_session.failure(db,generic[0],'invalid_request')
            prior=db.execute('SELECT * FROM task_trigger_tickets WHERE incoming_wire_id=?',(data['incoming_wire_id'],)).fetchone()
            if prior is not None:
                if prior['task_id']!=data['id'] or prior['incoming_utc']!=data['incoming_utc']:raise ValueError('actual wire already bound')
                budget=b.TurnBudget(json.loads(prior['budget_json']))
                output={'status':'existing_ticket','ticket':prior['ticket'],'stage':prior['stage'],'new_budget':False,'data_only':True}
                try:budget.admit(output)
                except b.ContextOverflow:
                    try:output=budget.admit_failure('context_overflow')
                    except b.ContextOverflow:raise BudgetExhausted('no further ticket output') from None
                save_budget(db,prior['ticket'],budget);return output
            source=s._row(db,'task',data['id']);ticket=secrets.token_hex(24);budget=b.TurnBudget()
            db.execute('INSERT INTO task_trigger_tickets VALUES (?,?,?,?,?,?,?,\'open\',0,?,NULL,NULL)',
              (ticket,data['incoming_wire_id'],data['id'],data['incoming_utc'],source['revision'],source['original_digest'],
               s.utc_now(),s.compact_json(budget.snapshot())))
            n.bump(db)
            try:
                evidence=triggers.get(db,{'id':data['id']})
                output={'status':'review_required','ticket':ticket,'source':evidence,'data_only':True}
                budget.admit(output,source_keys=('source',));save_budget(db,ticket,budget);return output
            except (ValueError,KeyError,TypeError,sqlite3.Error) as error:
                return failure(db,ticket,str(error) if isinstance(error,b.ContextOverflow) else 'invalid_request')
    if set(request)!={'continue_task_trigger'} or cfg is None:raise ValueError('configured bounded trigger continuation required')
    data=request['continue_task_trigger']
    if not isinstance(data,dict) or not isinstance(data.get('ticket'),str):raise ValueError('host-created ticket required')
    ticket=data['ticket'];row=db.execute('SELECT * FROM task_trigger_tickets WHERE ticket=?',(ticket,)).fetchone()
    if row is None:raise ValueError('unknown host-created ticket')
    action=data.get('action')
    try:
        with s.atomic(db,True),s.bounded_work(db):
            row=db.execute('SELECT * FROM task_trigger_tickets WHERE ticket=?',(ticket,)).fetchone()
            if row['stage'] in ('complete','held'):raise ValueError('ticket closed')
            if action!='recover' and datetime.now(timezone.utc)-datetime.fromisoformat(row['created_utc'][:-1]+'+00:00')>timedelta(minutes=15):
                raise ValueError('finite actual-wake continuation expired')
            budget=b.TurnBudget(json.loads(row['budget_json']))
            current=s._row(db,'task',row['task_id'])
            if current['revision']!=row['source_revision'] or current['original_digest']!=row['source_digest']:
                raise SourceChanged('current source changed after semantic review')
            if action=='provenance':
                if set(data)!={'ticket','action','key'} or row['stage']!='open':raise ValueError('one selected provenance read only')
                projection=triggers.get(db,{'id':row['task_id'],'provenance_key':data['key']})
                output={'status':'review_required','source':projection['selected_provenance'],'data_only':True}
                budget.admit(output,source_keys=('source',));save_budget(db,ticket,budget)
                db.execute("UPDATE task_trigger_tickets SET stage='provenance' WHERE ticket=?",(ticket,));return output
            if action=='reserve':
                required={'ticket','action','expected_revision','digest','permission_wire_id','permission_confirmed','match_confirmed'}
                if set(data)!=required or row['stage'] not in ('open','provenance'):raise ValueError('one reviewed exact reservation only')
                if data['expected_revision']!=row['source_revision'] or data['digest']!=row['source_digest']:
                    raise ValueError('exact reviewed snapshot required')
                reserve=triggers.reserve(db,{'id':row['task_id'],'expected_revision':data['expected_revision'],
                  'digest':data['digest'],'incoming_wire_id':row['incoming_wire_id'],'incoming_utc':row['incoming_utc'],
                  'permission_wire_id':data['permission_wire_id'],'permission_confirmed':data['permission_confirmed'],
                  'match_confirmed':data['match_confirmed']})
                base=s.compact_json(budget.snapshot())
                # Reserve space before any cross-store mutation; final emission is
                # charged exactly once, or recovery burns the unused reservation.
                budget.admit({'status':'partial','source':reserve|{'reserved_source':'x'*32},
                              'return':{'reserved_control':'x'*500},'data_only':True},source_keys=('source',))
                save_budget(db,ticket,budget)
                db.execute("UPDATE task_trigger_tickets SET stage='pending',attempts=1,base_budget_json=?,reservation_json=? WHERE ticket=?",
                           (base,s.compact_json(reserve),ticket))
            elif action=='recover':
                if set(data)!={'ticket','action'} or row['stage']!='pending' or row['attempts']>=2:raise ValueError('one exact interrupted finish reconciliation only')
                reserve=json.loads(row['reservation_json'])|{'initial_response_allowed':False}
                db.execute('UPDATE task_trigger_tickets SET attempts=attempts+1 WHERE ticket=?',(ticket,))
            else:raise ValueError('finite continuation action required')
        finish=triggers.finish(db,cfg,{'id':row['task_id'],'incoming_wire_id':row['incoming_wire_id']})
        output={'status':'partial' if finish['status'].startswith('held_') else 'ok','return':finish,'data_only':True}
        if action=='reserve' or row['base_budget_json'] is not None:output['source']=reserve
        else:output['initial_response_allowed']=False
        with s.atomic(db,True):
            current=db.execute('SELECT * FROM task_trigger_tickets WHERE ticket=?',(ticket,)).fetchone()
            # The conservative reservation is never refunded/reset. A crash
            # recovery can consume the reserved un-emitted envelope once.
            if current['base_budget_json'] is None:
                budget=b.TurnBudget(json.loads(current['budget_json']));budget.admit(output,source_keys=('source',));save_budget(db,ticket,budget)
            else:
                # Source call was reserved already; complete emission is also
                # accounted without a local size check or renewed source call.
                budget=b.TurnBudget(json.loads(current['budget_json']))
                budget.admit(output,whole_slot='control');save_budget(db,ticket,budget)
            db.execute('UPDATE task_trigger_tickets SET stage=?,base_budget_json=NULL WHERE ticket=?',
                       ('pending' if output['status']=='partial' else 'complete',ticket))
        return output
    except (ValueError,OSError,KeyError,TypeError,sqlite3.Error) as error:
        return failure(db,ticket,'source_changed' if isinstance(error,SourceChanged) else str(error) if isinstance(error,b.ContextOverflow) else 'invalid_request')
