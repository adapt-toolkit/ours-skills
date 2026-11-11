#!/usr/bin/env python3
"""One bounded ordinary-intake batch. No raw exports, SQL, schedules or sends."""
import argparse
import json
from pathlib import Path
import sqlite3
import sys

import context_budget as budget
import semantic_recall
import preference_tools
import note_store
import note_embedding
import return_tools
import task_trigger_session
import source_store as source
from legacy_source import pairs, read_private_json

READS = {'metadata','context','task_get','event_get','list','search','note_get','note_list','note_search','note_semantic','note_themes','note_match','return_get','task_trigger_get'}
WRITES = {'task_put','event_put','task_status','event_cancel','restore','note_put','note_forget','note_archive','note_restore','note_resolve','note_index','note_rule_put','note_rule_cancel','note_rule_use','note_rule_rebind','return_plan','return_commit','task_trigger_reserve'}


def dispatch(db,call,embed,verify):
    action=call['action'];args=call['args']
    if action.startswith('note_'):return note_store.run(db,action,args,embed)
    if action.startswith('return_'):return return_tools.run(db,action,args,verify)
    if action.startswith('task_trigger_'):
        import task_triggers
        note_store.check(db)
        return task_triggers.get(db,args) if action=='task_trigger_get' else task_triggers.reserve(db,args)
    return source.run(db,action,args)


def run(db, request, fetch=None, embed=None, verify=None, cfg=None):
    if isinstance(request,dict) and ('prepare_message_return' in request or 'continue_message_return' in request):
        import message_return_session
        return message_return_session.run(db,request)
    if isinstance(request,dict) and ('prepare_task_trigger' in request or 'continue_task_trigger' in request):
        import task_trigger_session
        return task_trigger_session.run(db,request,cfg)
    if isinstance(request,dict) and 'finish_return' in request:
        import return_intake
        return return_intake.run(db,request,cfg,embed,verify)
    if isinstance(request,dict) and 'finish_task_trigger' in request:
        import task_triggers
        if set(request)!={'calls','finish_task_trigger'} or cfg is None:
            raise ValueError('one configured task trigger batch required')
        finish=request['finish_task_trigger']
        if not isinstance(finish,dict) or set(finish)!={'id','incoming_wire_id'}:raise ValueError('exact task trigger finish required')
        with source.atomic(db,True):
            output=run(db,{'calls':request['calls']},embed=embed,verify=verify)
            saved=db.execute('SELECT 1 FROM task_trigger_reservations WHERE task_id=? AND incoming_wire_id=?',
                             (finish['id'],finish['incoming_wire_id'])).fetchone()
            if saved is None:raise ValueError('exact winning task trigger required')
            budget.TurnBudget().admit(output|{'return':{'reserved_control':'x'*700}},source_keys=('source',))
        output['return']=task_triggers.finish(db,cfg,finish)
        if output['return']['status'].startswith('held_'):output['status']='partial'
        budget.TurnBudget().admit(output,source_keys=('source',))
        return output
    if not isinstance(request,dict) or set(request)-{'calls','memory_topic','preferences'}:
        raise ValueError('bounded intake request required')
    calls=request.get('calls',[]);topic=request.get('memory_topic')
    if not isinstance(calls,list) or not 1<=len(calls)<=3:
        raise ValueError('one to three source calls required')
    for call in calls:
        if (not isinstance(call,dict) or set(call)!={'action','args'} or
                call['action'] not in READS|WRITES or not isinstance(call['args'],dict)):
            raise ValueError('invalid source call')
    if sum(call['action'] in ('list','search','note_list','note_search','note_semantic','note_themes','note_match') for call in calls)>1:
        raise ValueError('one list/search page per explicit intake batch')
    embedding_calls=sum(c['action'] in ('note_index','note_semantic','note_themes') for c in calls)
    if embedding_calls>1:raise ValueError('one offline embedding operation per batch')
    writes=any(call['action'] in WRITES for call in calls)
    if topic is not None and writes:
        raise ValueError('semantic recall is unnecessary for source mutations')
    # One source snapshot or one atomic CAS batch. No output before combined
    # cap validation; oversized final write acknowledgement rolls everything back.
    ticket=budget.TurnBudget()
    with source.atomic(db,writes):
        with source.bounded_work(db,seconds=12 if embedding_calls else 1):
            results=[{'action':call['action'],'result':dispatch(db,call,embed,verify)} for call in calls]
        output={'status':'ok','data_only':True,'source':results}
        if 'preferences' in request:
            output['preferences']=preference_tools.read(db,request['preferences'])
        if topic is not None:
            output['memory']=semantic_recall.recall(db,topic,fetch)
        ticket.admit(output,source_keys=('source',),memory_keys=('memory',))
    return output


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',required=True)
    opts=p.parse_args();db=None;request=None;bound=False
    try:
        raw=sys.stdin.buffer.read()
        request=json.loads(raw,object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))
        # Config/source namespace belong to installed host binding. Input never
        # selects a source path, recipient, memory scope or new budget identity.
        cfg=read_private_json(Path(opts.config))
        if cfg.get('source_schema')!=2 or not cfg.get('source_db') or cfg.get('source_json'):
            raise ValueError('indexed source binding required')
        db=source.connect(Path(cfg['source_db']))
        if source.meta(db,'namespace')!=cfg.get('source_namespace'):
            raise ValueError('source namespace binding mismatch')
        bound=True
        result=run(db,request,embed=note_embedding.binding(cfg),verify=return_tools.verifier(cfg),cfg=cfg)
        print(budget.compact_json(result));return 0
    except budget.ContextOverflow as e:
        if bound:
            try:held=task_trigger_session.request_failure(db,request,str(e))
            except task_trigger_session.BudgetExhausted:return 1
            if held is not None:
                print(budget.compact_json(held));return 1
        print(budget.compact_json(budget.failure(str(e))));return 1
    except task_trigger_session.BudgetExhausted:
        return 1  # Exhausted trusted ticket permits no further model output.
    except (ValueError,KeyError,TypeError,OSError,sqlite3.Error,UnicodeError):
        if bound:
            try:held=task_trigger_session.request_failure(db,request)
            except task_trigger_session.BudgetExhausted:return 1
            if held is not None:
                print(budget.compact_json(held));return 1
        print(budget.compact_json(budget.failure('invalid_request')));return 1
    finally:
        if db is not None:db.close()


if __name__=='__main__':sys.exit(main())
