"""One actual-message bounded match snapshot and one reviewed return reservation.

Persistent counters/opaque handles are host-owned bookkeeping, not authentication.
The trusted assistant verifies the real Owner wake and reviews saved evidence.
"""
import json
import secrets
from datetime import datetime,timezone,timedelta
import sqlite3

import source_store as s
import context_budget as b
from task_trigger_session import BudgetExhausted

SCHEMA='''
CREATE TABLE message_return_tickets(
 ticket TEXT PRIMARY KEY,incoming_wire_id TEXT NOT NULL UNIQUE,incoming_text TEXT NOT NULL,
 created_utc TEXT NOT NULL,stage TEXT NOT NULL CHECK(stage IN ('open','complete','held')),
 evidence_json TEXT,budget_json TEXT NOT NULL);
'''


def failure(db,ticket,reason='invalid_request'):
    with s.atomic(db,True):
        row=db.execute('SELECT budget_json FROM message_return_tickets WHERE ticket=?',(ticket,)).fetchone()
        budget=b.TurnBudget(json.loads(row[0]))
        try:output=budget.admit_failure(reason)
        except b.ContextOverflow:raise BudgetExhausted('no further ticket output') from None
        db.execute("UPDATE message_return_tickets SET stage=CASE WHEN stage='complete' THEN stage ELSE 'held' END,budget_json=? WHERE ticket=?",
                   (s.compact_json(budget.snapshot()),ticket))
        return output


def run(db,request):
    try:return _run(db,request)
    except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as error:
        from task_trigger_session import request_failure
        output=request_failure(db,request,str(error) if isinstance(error,b.ContextOverflow) else 'invalid_request')
        if output is not None:return output
        raise


def _run(db,request):
    import note_store as n
    n.check(db)
    if set(request)=={'prepare_message_return'}:
        data=request['prepare_message_return']
        if not isinstance(data,dict) or set(data)!={'incoming_wire_id','incoming_text'}:raise ValueError('exact actual match input required')
        n.text(data['incoming_wire_id'],128);n.words(data['incoming_text'])
        with s.atomic(db,True),s.bounded_work(db):
            task=db.execute('SELECT ticket FROM task_trigger_tickets WHERE incoming_wire_id=?',(data['incoming_wire_id'],)).fetchone()
            if task is not None:
                import task_trigger_session
                return task_trigger_session.failure(db,task[0],'invalid_request')
            prior=db.execute('SELECT * FROM message_return_tickets WHERE incoming_wire_id=?',(data['incoming_wire_id'],)).fetchone()
            if prior is not None:
                if prior['incoming_text']!=data['incoming_text']:return failure(db,prior['ticket'])
                budget=b.TurnBudget(json.loads(prior['budget_json']))
                output={'status':'existing_ticket','ticket':prior['ticket'],'stage':prior['stage'],'new_budget':False,'data_only':True}
                try:budget.admit(output)
                except b.ContextOverflow:
                    try:output=budget.admit_failure('context_overflow')
                    except b.ContextOverflow:raise BudgetExhausted('no further ticket output') from None
                db.execute('UPDATE message_return_tickets SET budget_json=? WHERE ticket=?',(s.compact_json(budget.snapshot()),prior['ticket']))
                return output
            ticket=secrets.token_hex(24);budget=b.TurnBudget()
            db.execute("INSERT INTO message_return_tickets VALUES (?,?,?,?,'open',NULL,?)",
                       (ticket,data['incoming_wire_id'],data['incoming_text'],s.utc_now(),s.compact_json(budget.snapshot())))
            n.bump(db)
            try:
                matches=n.matches(db,data)
                output={'status':'review_required' if matches['records'] else matches['status'],'ticket':ticket,'source':matches,'data_only':True}
                budget.admit(output,source_keys=('source',))
                if matches['records']:
                    # Reserve final space before emitting evidence, not a new
                    # budget after the assistant has chosen a candidate.
                    budget.admit({'status':'ok','source':{'reserved_source':'x'*350},'data_only':True},source_keys=('source',))
                db.execute('UPDATE message_return_tickets SET stage=?,evidence_json=?,budget_json=? WHERE ticket=?',
                  ('open' if matches['records'] else 'held' if matches.get('hold') else 'complete',
                   s.compact_json(matches),s.compact_json(budget.snapshot()),ticket))
                return output
            except (ValueError,KeyError,TypeError,sqlite3.Error) as error:
                # Save conservative charges already made, even on a preflight hold.
                db.execute('UPDATE message_return_tickets SET budget_json=? WHERE ticket=?',(s.compact_json(budget.snapshot()),ticket))
                return failure(db,ticket,str(error) if isinstance(error,b.ContextOverflow) else 'invalid_request')
    if set(request)!={'continue_message_return'}:raise ValueError('one generic match workflow required')
    data=request['continue_message_return']
    required={'ticket','rule_id','expected_revision','source_revision','source_digest','match_confirmed'}
    if not isinstance(data,dict) or set(data)!=required or not isinstance(data['ticket'],str):raise ValueError('exact reviewed match required')
    ticket=data['ticket']
    if db.execute('SELECT 1 FROM message_return_tickets WHERE ticket=?',(ticket,)).fetchone() is None:raise ValueError('unknown host ticket')
    try:
        with s.atomic(db,True),s.bounded_work(db):
            row=db.execute('SELECT * FROM message_return_tickets WHERE ticket=?',(ticket,)).fetchone()
            if row['stage']!='open' or data['match_confirmed'] is not True:raise ValueError('current reviewed actual match required')
            if datetime.now(timezone.utc)-datetime.fromisoformat(row['created_utc'][:-1]+'+00:00')>timedelta(minutes=15):
                raise ValueError('finite review window expired')
            matches=json.loads(row['evidence_json']);selected=next((v for v in matches['records'] if v['rule_id']==data['rule_id']),None)
            if selected is None:raise ValueError('select one complete returned rule only')
            kind='task' if 'task' in selected else 'note';source=selected[kind]
            if (type(data['expected_revision']) is not int or data['expected_revision']!=selected['rule_revision']
                    or type(data['source_revision']) is not int or data['source_revision']!=source['revision']
                    or data['source_digest']!=source['digest']):raise ValueError('exact returned snapshot required')
            ack=n.rule_use(db,{'id':selected['rule_id'],'expected_revision':data['expected_revision'],
              kind+'_revision':data['source_revision'],kind+'_digest':data['source_digest'],
              'incoming_wire_id':row['incoming_wire_id'],'incoming_text':row['incoming_text']})
            output={'status':'ok','source':ack,'data_only':True}
            # Reserved call permission remains consumed; account complete actual
            # output additionally without another source-call admission.
            budget=b.TurnBudget(json.loads(row['budget_json']))
            budget.admit(output,whole_slot='control')
            db.execute('UPDATE message_return_tickets SET budget_json=? WHERE ticket=?',
                       (s.compact_json(budget.snapshot()),ticket))
            db.execute("UPDATE message_return_tickets SET stage='complete' WHERE ticket=?",(ticket,));n.bump(db)
            return output
    except (ValueError,KeyError,TypeError,sqlite3.Error) as error:
        return failure(db,ticket,str(error) if isinstance(error,b.ContextOverflow) else 'source_changed')
