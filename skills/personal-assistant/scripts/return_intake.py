"""One bounded intake envelope including one finite return's deterministic finish.

Message source/plan/rule/ack commit atomically. A timed finish crosses the existing
durable journal; source and ledger cannot be one SQLite transaction. A failure
after source commit is explicitly partial and the same immutable identity resumes.
"""
import json
from pathlib import Path
import sqlite3

import context_budget as b
import source_store as s
import note_store as n
import return_tools as r


def run(db,request,cfg,embed,verify):
    import intake_tools
    if set(request)!={'calls','finish_return'}:raise ValueError('one bounded intake and finish required')
    finish=request['finish_return']
    common={'kind','ref','expected_source_revision','expected_revision'}
    if not isinstance(finish,dict) or finish.get('kind') not in ('message','timed'):raise ValueError('finite finish kind required')
    extra='rule' if finish['kind']=='message' else 'timer'
    if set(finish)!=common|{extra}:raise ValueError('exact return finish required')
    calls=request['calls']
    if not isinstance(calls,list) or not 1<=len(calls)<=3:raise ValueError('one to three intake calls required')
    if any(not isinstance(c,dict) for c in calls):raise ValueError('typed source calls required')
    if any(c.get('action') in ('return_commit','note_rule_put','note_rule_use') for c in calls):
        raise ValueError('finish owns its single materialization and acknowledgement')
    if finish['kind']=='timed' and cfg is None:raise ValueError('configured journal required')
    commit={k:finish[k] for k in ('ref','expected_revision','expected_source_revision')}
    result=None
    # Account the complete combined envelope without artificial size ceilings.
    # Record a conservative control reservation before source commits.
    with s.atomic(db,True):
        result=intake_tools.run(db,{'calls':calls},embed=embed,verify=verify)
        plan=db.execute('SELECT * FROM return_decisions WHERE source_ref=?',(finish['ref'],)).fetchone()
        if (plan is None or plan['decision'] not in ('planned_'+finish['kind'],'committed_'+finish['kind'])
                or plan['source_revision']!=finish['expected_source_revision']
                or plan['revision']!=finish['expected_revision']):raise ValueError('exact current finish plan required')
        s.identifier(plan['mechanism_id'])
        if finish['kind']=='message':
            rule=finish['rule']
            if not isinstance(rule,dict) or rule.get('id')!=plan['mechanism_id']:raise ValueError('exact planned rule required')
            prior=db.execute('SELECT original_json FROM note_rules WHERE id=?',(plan['mechanism_id'],)).fetchone()
            if prior is None:n.rule_put(db,rule)
            elif json.loads(prior[0])!=rule.get('record'):raise ValueError('exact rule recovery required')
            with s.bounded_work(db):ack=r.commit(db,commit,verify)
            result['return']=ack
            b.TurnBudget().admit(result,source_keys=('source',))
            return result
        timer=finish['timer']
        if (not isinstance(timer,dict) or timer.get('operation_id')!=plan['mechanism_id']
                or timer.get('expected_source_revision')!=finish['expected_source_revision']
                or timer.get('source_ref')!=finish['ref'] or timer.get('mode')!='contextual'
                or timer.get('followup_limit',0)!=0 or 'owner_request_wire_id' in timer):
            raise ValueError('exact delegated timer required')
        b.TurnBudget().admit(result|{'return':{'reserved_control':'x'*700}},source_keys=('source',))
    try:
        import reminder_tools as runtime
        store=runtime.Store(Path(cfg['database']))
        try:receipt=runtime.mutate_with_source_journal(store,cfg,'create',timer)
        finally:store.db.close()
        commit['reminder_id']=receipt['id']
        with s.atomic(db,True),s.bounded_work(db):ack=r.commit(db,commit,verify)
        result['return']=ack|{'reminder_id':receipt['id']}
    except (ValueError,OSError,KeyError,TypeError,sqlite3.Error):
        # No retry or alternate timer. Exact journal identity is recovery authority.
        result['status']='partial'
        result['return']={'status':'held_reconciliation_required','source_committed':True,
                          'return_committed':False,'mechanism_state':'unverified',
                          'mechanism_id':plan['mechanism_id'],'delivery_unconfirmed':True}
    b.TurnBudget().admit(result,source_keys=('source',))
    return result
