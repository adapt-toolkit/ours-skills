#!/usr/bin/env python3
"""Private explicit source/reminder operation journal; never infers a timer or sends.

Source hooks must persist idempotent prepare/map acknowledgements transactionally.
Ledger receipts and mutation commit together, with a due hold until mapping ACK.
"""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


class Journal:
    def __init__(self,path):
        path=Path(path)
        if not path.is_absolute() or path.is_symlink():raise ValueError('private absolute journal path required')
        parent=path.parent
        info=parent.stat()
        if parent.is_symlink() or info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=0o700:
            raise ValueError('journal parent requires private owner and mode700')
        try:
            fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600);os.close(fd)
        except FileExistsError:pass
        info=path.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=0o600:
            raise ValueError('private regular journal required')
        self.db=sqlite3.connect(path,timeout=10,isolation_level=None)
        self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('''CREATE TABLE IF NOT EXISTS operations(
            operation_id TEXT PRIMARY KEY,request_digest TEXT NOT NULL,request_json TEXT NOT NULL,
            stage TEXT NOT NULL CHECK(stage IN ('requested','prepared','ledger','mapped','complete','held')),
            result_json TEXT,error_code TEXT)''')
        self.db.execute('CREATE INDEX IF NOT EXISTS operations_stage_id ON operations(stage,operation_id)')

    def transaction(self,fn):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            result=fn();self.db.execute('COMMIT');return result
        except BaseException:
            self.db.execute('ROLLBACK');raise

    def get(self,operation_id):
        row=self.db.execute('SELECT * FROM operations WHERE operation_id=?',(operation_id,)).fetchone()
        if row is None:raise ValueError('unknown source schedule operation')
        return dict(row)

    def request(self,operation_id,source_ref,expected_source_revision,action,args):
        if (not isinstance(operation_id,str) or not 1<=len(operation_id)<=128
            or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.' for c in operation_id)):
            raise ValueError('stable ASCII operation_id required')
        if (not isinstance(source_ref,str) or not source_ref.startswith(('task:','event:','note:'))
                or not source_ref.split(':',1)[1] or len(source_ref)>134):
            raise ValueError('exact source reference required')
        if type(expected_source_revision) is not int or expected_source_revision<1:
            raise ValueError('expected source revision required')
        if action not in ('create','update','cancel','snooze','create_followup') or not isinstance(args,dict):
            raise ValueError('explicit reminder mutation required')
        # Binding is exact: a source-bound timer cannot point at a different record.
        if action=='create' and args.get('source_ref')!=source_ref:
            raise ValueError('new timer must bind the explicit source reference')
        if 'source_ref' in args and args['source_ref']!=source_ref:
            raise ValueError('source schedule binding mismatch')
        request={'operation_id':operation_id,'source_ref':source_ref,'expected_source_revision':expected_source_revision,
                 'action':action,'args':args}
        encoded=canonical(request)
        digest=hashlib.sha256(encoded.encode()).hexdigest()
        def save():
            old=self.db.execute('SELECT request_digest FROM operations WHERE operation_id=?',(operation_id,)).fetchone()
            if old is not None:
                if old[0]!=digest:raise ValueError('operation_id conflict; reconcile before retry')
            else:self.db.execute('INSERT INTO operations VALUES (?,?,?,\'requested\',NULL,NULL)',(operation_id,digest,encoded))
            return {'operation_id':operation_id,'stage':self.get(operation_id)['stage'],'request_digest':digest}
        return self.transaction(save)

    def run(self,operation_id,ledger,source,now=None,fault=None,context_reader=None):
        """Resume a known request only. source.prepare_schedule/map_schedule are CAS hooks.

        A raised crash fault leaves the last durable stage; ordinary refusals HOLD.
        Source hooks receive the immutable request and its journal digest; map also
        receives exact ledger id/revision. No source hook may schedule/send itself.
        """
        def checkpoint(stage,result=None):
            self.transaction(lambda:self.db.execute('UPDATE operations SET stage=?,result_json=COALESCE(?,result_json),error_code=NULL WHERE operation_id=?',
                (stage,None if result is None else canonical(result),operation_id)))
            if fault is not None:fault(stage)
        row=self.get(operation_id)
        if row['stage']=='held':raise ValueError('operation held; explicit private reconciliation required')
        request=json.loads(row['request_json']);digest=row['request_digest']
        if row['stage']=='complete':return {'status':'complete','result':json.loads(row['result_json'])}
        try:
            if row['stage']=='requested':
                source.prepare_schedule(request,digest)
                if fault is not None:fault('source_prepare_committed')
                checkpoint('prepared');row=self.get(operation_id)
            if row['stage']=='prepared':
                if request['action']=='create_followup':
                    result=ledger.create_followup(request['args'],now,context_reader,operation_id=operation_id)
                else:result=ledger.tool(request['action'],request['args'],now,operation_id=operation_id)
                if fault is not None:fault('ledger_receipt_committed')
                checkpoint('ledger',result);row=self.get(operation_id)
            result=json.loads(row['result_json'])
            receipt=ledger.receipt(operation_id)
            expected=hashlib.sha256(canonical({'action':request['action'],'args':request['args']}).encode()).hexdigest()
            if receipt is None or receipt['request_sha256']!=expected or receipt['result']!=result:
                raise ValueError('ledger operation receipt mismatch')
            if row['stage']=='ledger':
                source.map_schedule(request,digest,result)
                if fault is not None:fault('source_mapping_committed')
                checkpoint('mapped');row=self.get(operation_id)
            if row['stage']=='mapped':
                ledger.complete_operation(operation_id)
                if fault is not None:fault('ledger_hold_released')
                checkpoint('complete',result)
            return {'status':'complete','result':result}
        except (ValueError,sqlite3.Error):
            # No raw exception or request content is exposed/stored as a reason.
            self.transaction(lambda:self.db.execute("UPDATE operations SET stage='held',error_code='reconciliation_required' WHERE operation_id=?",(operation_id,)))
            raise ValueError('source schedule operation held; reconcile privately') from None

    def pending(self,limit=25):
        if type(limit) is not int or not 1<=limit<=25:raise ValueError('pending page limit must be1..25')
        return [dict(r) for r in self.db.execute("SELECT operation_id,stage,error_code FROM operations WHERE stage!='complete' ORDER BY operation_id LIMIT ?",(limit,))]
