#!/usr/bin/env python3
"""Operator-only private source migration. No service control, admission or sends.

Use only after exact reciprocal review, fresh PA sole-writer pause and helper stop.
The scheduler lock is verified; a flag alone cannot prove the PA writer is paused.
"""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile


class Refusal(ValueError):pass
REVIEWED_PREVIOUS_SOURCE_SHA='019b2eab5c3467bff5410796e24b8a6a9761f0328e07dde0ee76d083253605b5'
def require(ok,code):
    if not ok:raise Refusal(code)
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def pairs(items):
    result={}
    for key,value in items:
        require(key not in result,'duplicate_json_key');result[key]=value
    return result
def decode(raw):
    return json.loads(raw.decode('utf8'),object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(Refusal('nonfinite_json')))
def private_dir(path):
    path=Path(path);require(path.is_absolute() and not path.is_symlink(),'private_directory_required')
    info=path.stat();require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.getuid() and stat.S_IMODE(info.st_mode)==0o700,'private_directory_required')
    return path
def private_file(path):
    path=Path(path);require(path.is_absolute(),'absolute_private_file_required')
    info=path.stat(follow_symlinks=False)
    require(stat.S_ISREG(info.st_mode) and info.st_uid==os.getuid() and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o600,'private_regular_file_required')
    return path
def read_private(path,limit=None):
    path=private_file(path);fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        a=os.fstat(fd)
        require(stat.S_ISREG(a.st_mode) and a.st_uid==os.getuid() and a.st_nlink==1 and stat.S_IMODE(a.st_mode)==0o600,'private_regular_file_required')
        with os.fdopen(fd,'rb',closefd=False) as file:raw=file.read() if limit is None else file.read(limit+1)
        z=os.fstat(fd);current=path.stat(follow_symlinks=False)
        sig=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        require(limit is None or len(raw)<=limit,'private_input_exceeds_bound')
        require(sig(a)==sig(z)==sig(current),'private_input_changed')
        return raw
    finally:os.close(fd)
def sync_dir(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)
def create_private(path,raw):
    path=Path(path);private_dir(path.parent)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as file:file.write(raw);file.flush();os.fsync(file.fileno())
    sync_dir(path.parent)
def artifact_digest(path):
    """Private operator artifact only: stable streaming bytes, no input cap lift."""
    path=private_file(path);fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        before=os.fstat(fd);h=hashlib.sha256()
        with os.fdopen(fd,'rb',closefd=False) as file:
            while True:
                block=file.read(65536)
                if not block:break
                h.update(block)
        after=os.fstat(fd);current=path.stat(follow_symlinks=False)
        signature=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        require(signature(before)==signature(after)==signature(current),'private_artifact_changed')
        return h.hexdigest()
    finally:os.close(fd)
def preserve_json_artifact(path,value):
    encoder=json.JSONEncoder(sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
    digest=hashlib.sha256();file=None
    if not Path(path).exists():
        private_dir(Path(path).parent)
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);file=os.fdopen(fd,'wb')
    try:
        for part in encoder.iterencode(value):
            raw=part.encode('utf8');digest.update(raw)
            if file is not None:file.write(raw)
        digest.update(b'\n')
        if file is not None:file.write(b'\n');file.flush();os.fsync(file.fileno())
    finally:
        if file is not None:file.close();sync_dir(Path(path).parent)
    require(artifact_digest(path)==digest.hexdigest(),'private_recovery_artifact_equality_failed')
def replace_private(path,raw,expected_sha):
    path=private_file(path);require(sha(read_private(path))==expected_sha,'atomic_replace_compare_failed')
    private_dir(path.parent)
    fd,name=tempfile.mkstemp(prefix='.migration-',dir=path.parent)
    with os.fdopen(fd,'wb') as file:file.write(raw);file.flush();os.fsync(file.fileno())
    require(sha(read_private(path))==expected_sha,'atomic_replace_compare_failed')
    os.replace(name,path);sync_dir(path.parent)
def ro_db(path):
    db=sqlite3.connect(private_file(path).as_uri()+'?mode=ro',uri=True,timeout=5,isolation_level=None)
    db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');return db
def quote(name):return '"'+name.replace('"','""')+'"'
def ledger_shape(db):
    tables={r[0]:[r[1] for r in db.execute('PRAGMA table_info('+quote(r[0])+')')]
            for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")}
    require({'reminders','events','followups'}<=set(tables),'unexpected_ledger_schema')
    return tables
def table_digest(db,table,columns):
    require(set(columns)<={r[1] for r in db.execute('PRAGMA table_info('+quote(table)+')')},'original_ledger_field_missing')
    h=hashlib.sha256();count=0
    fields=','.join(quote(c) for c in columns)
    for row in db.execute('SELECT '+fields+' FROM '+quote(table)+' ORDER BY '+fields):
        raw=canonical([{'sqlite_blob_hex':v.hex()} if isinstance(v,bytes) else v for v in row]).encode()
        h.update(len(raw).to_bytes(8,'big'));h.update(raw);count+=1
    return {'count':count,'sha256':h.hexdigest(),'columns':columns}
def ledger_snapshot(db,shape=None):
    require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok','ledger_integrity_failed')
    shape=ledger_shape(db) if shape is None else shape
    return {table:table_digest(db,table,cols) for table,cols in shape.items()}
def quiet(db):
    require(not db.execute("SELECT 1 FROM events WHERE status IN('submitting','uncertain') OR delivery_status IN('sending','uncertain') LIMIT 1").fetchone(),'uncertain_attempt_reconcile_first')
    require(not db.execute("""SELECT 1 FROM events e JOIN reminders r ON r.id=e.reminder_id
        WHERE r.mode='contextual' AND e.status='queued' AND e.delivery_status IS NULL LIMIT 1""").fetchone(),'admitted_contextual_draft_hold_reconcile_first')
def backup_database(path,target):
    original=ro_db(path)
    try:
        original.execute('BEGIN');create_private(target,b'');saved=sqlite3.connect(target)
        try:original.backup(saved);saved.commit()
        finally:saved.close();original.execute('ROLLBACK')
    finally:original.close()
    fd=os.open(target,os.O_RDONLY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)
    sync_dir(Path(target).parent)
@contextmanager
def quiescent(cfg,ack):
    require(ack,'fresh_operator_quiescence_ack_required')
    ledger=private_file(cfg['database']);private_dir(ledger.parent)
    path=Path(str(ledger)+'.scheduler.lock')
    fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    try:
        info=os.fstat(fd);require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.getuid() and stat.S_IMODE(info.st_mode)==0o600,'private_scheduler_lock_required')
        try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise Refusal('helper_still_running') from None
        db=ro_db(ledger)
        try:quiet(db)
        finally:db.close()
        yield
    finally:os.close(fd)
def load_module(path,expected_sha,name):
    path=Path(path)
    require(path.is_absolute() and path.is_file() and not path.is_symlink() and sha(path.read_bytes())==expected_sha,'reviewed_module_hash_required')
    sys.path.insert(0,str(path.parent))
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module
def verify_bundle(root,manifest,manifest_sha):
    root=Path(root);manifest=Path(manifest)
    require(root.is_absolute() and root.is_dir() and not root.is_symlink(),'reviewed_bundle_required')
    require(manifest.is_absolute() and manifest.is_file() and not manifest.is_symlink(),'reviewed_manifest_required')
    raw=manifest.read_bytes();require(len(raw)<=65536 and sha(raw)==manifest_sha,'reviewed_manifest_hash_required')
    files=decode(raw).get('files');require(isinstance(files,dict) and bool(files),'reviewed_manifest_files_required')
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and '__pycache__' not in p.parts}
    require(actual==set(files),'reviewed_bundle_file_set_changed')
    for name,digest in files.items():
        rel=Path(name);require(not rel.is_absolute() and '..' not in rel.parts,'reviewed_manifest_path_invalid')
        path=root/rel
        require(all(not p.is_symlink() for p in (path,*path.parents) if p!=root.parent),'reviewed_bundle_symlink_refused')
        require(path.is_file() and sha(path.read_bytes())==digest,'reviewed_bundle_dependency_changed')
    return {'verified_files':len(files),'manifest_sha256':manifest_sha}

def verify_import_bindings(source,db,ledger_path):
    count=0;mappings=0;historical=0;ledger=ro_db(ledger_path)
    try:
        ledger.execute('BEGIN');quiet(ledger)
        for row in ledger.execute('SELECT id,revision,source_ref,status FROM reminders WHERE source_ref IS NOT NULL ORDER BY id'):
            mapped=db.execute('SELECT * FROM reminder_mappings WHERE reminder_id=?',(row['id'],)).fetchone()
            history=db.execute('SELECT * FROM historical_terminal_bindings WHERE reminder_id=?',(row['id'],)).fetchone()
            ref=row['source_ref'];require(isinstance(ref,str),'imported_ledger_reference_invalid')
            kind,key=ref.split(':',1) if ':' in ref else ('','')
            reason='unsupported_reference' if kind not in ('event','task') or not key.strip() or len(key)>128 else (
                None if db.execute('SELECT 1 FROM '+kind+'s WHERE id=?',(key,)).fetchone() else 'source_missing')
            require(source.historical_binding_reason(db,ref)==reason,'historical_binding_classification_mismatch')
            if reason is not None:
                require(row['status'] in ('transport_confirmed','dismissed','cancelled','expired','invalidated'),
                        'unresolved_nonterminal_binding_hold')
                require(mapped is None and history is not None,'historical_binding_partition_mismatch')
                request={'historical_terminal_binding':True,'reminder_id':row['id'],'reminder_revision':row['revision'],
                         'source_ref':ref,'status':row['status'],'reason':reason}
                receipt_id='historical-import:'+source.digest({'namespace':source.meta(db,'namespace'),'reminder_id':row['id']})
                require(dict(history)=={'reminder_id':row['id'],'source_ref':ref,'reminder_revision':row['revision'],
                        'status':row['status'],'reason':reason,'receipt_id':receipt_id,'receipt_json':canonical(request),
                        'receipt_digest':source.digest(request)},'historical_terminal_receipt_mismatch')
                historical+=1;count+=1;continue
            require(history is None,'resolved_binding_has_historical_receipt')
            require(mapped is not None and mapped['source_ref']==row['source_ref'] and mapped['reminder_revision']==row['revision'] and mapped['status']==row['status'],'imported_ledger_binding_mismatch')
            kind,key=row['source_ref'].split(':',1);original=source._row(db,kind,key)
            require(mapped['source_revision']==original['revision'] and mapped['source_digest']==original['original_digest'],'imported_binding_source_mismatch')
            receipt=db.execute('SELECT * FROM source_schedule_operations WHERE operation_id=?',(mapped['operation_id'],)).fetchone()
            request={'imported_ledger_binding':True,'reminder_id':row['id'],'reminder_revision':row['revision'],'source_ref':row['source_ref'],'status':row['status']}
            require(receipt is not None and receipt['status']=='mapped' and receipt['digest']==source.digest(request) and receipt['result_digest']==source.digest(request),'imported_binding_receipt_mismatch')
            mappings+=1;count+=1
        require(db.execute('SELECT count(*) FROM reminder_mappings').fetchone()[0]==mappings and
                db.execute('SELECT count(*) FROM historical_terminal_bindings').fetchone()[0]==historical and
                db.execute('SELECT count(*) FROM source_schedule_operations').fetchone()[0]==mappings,
                'unexpected_imported_bindings')
    finally:ledger.close()
    return count
def field_counts(value):
    result={'objects':0,'arrays':0,'fields':0,'scalars':0}
    def walk(item):
        if isinstance(item,dict):
            result['objects']+=1;result['fields']+=len(item)
            for sub in item.values():walk(sub)
        elif isinstance(item,list):
            result['arrays']+=1
            for sub in item:walk(sub)
        else:result['scalars']+=1
    walk(value);return result
def checkpoint(folder):return decode(read_private(folder/'checkpoint.json'))
def update_checkpoint(folder,value):
    path=folder/'checkpoint.json';replace_private(path,(canonical(value)+'\n').encode(),sha(read_private(path)))
def resume_reviewed_import(folder,cp,target,source,source_sha,previous_sha,document,cfg,fault=None):
    """One reviewed importer correction, retaining the committed original import.

    This is not a generic hash override. It recognizes only the prior frozen
    helper and unchanged, fully imported generation1 with no mapping commits.
    All original files/config/ledger are checked by the caller under the lock.
    """
    require(previous_sha==REVIEWED_PREVIOUS_SOURCE_SHA and previous_sha!=source_sha,
            'reviewed_previous_source_hash_required')
    require(cp['stage']=='import_prepared' and cp.get('source_db')==str(target),
            'reviewed_import_repair_stage_required')
    artifact=folder/'checkpoint-before-reviewed-import-repair.json'
    if cp.get('source_helper_sha')==source_sha:
        require(cp.get('repair_from_source_sha')==previous_sha and
                sha(read_private(artifact))==cp.get('repair_checkpoint_sha256'),
                'reviewed_import_repair_provenance_failed')
        require(artifact_digest(folder/'source-before-reviewed-import-repair.sqlite')==cp.get('repair_source_backup_sha256'),
                'reviewed_import_repair_backup_changed')
        db=ro_db(target)
        try:source.recognize_import_schema(db)
        finally:db.close()
        return
    require(cp.get('source_helper_sha')==previous_sha and target.exists(),
            'reviewed_import_repair_target_required')
    db=ro_db(target)
    try:
        db.execute('BEGIN')
        source.recognize_import_schema(db)
        require(source.meta(db,'schema_version')=='2' and source.meta(db,'namespace')==cfg['source_namespace'] and
                source.meta(db,'generation')=='1' and source.meta(db,'legacy_imported')=='true',
                'reviewed_import_repair_state_required')
        exported=source.export_legacy(db)
        require(exported==document and field_counts(exported)==cp['source_field_counts'] and
                sha(canonical(exported).encode())==cp['source_canonical_digest'],
                'reviewed_import_repair_originals_changed')
        require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok' and not db.execute('PRAGMA foreign_key_check').fetchone(),
                'reviewed_import_repair_integrity_failed')
        require(not db.execute('SELECT 1 FROM reminder_mappings LIMIT 1').fetchone() and
                not db.execute('SELECT 1 FROM source_schedule_operations LIMIT 1').fetchone(),
                'reviewed_import_repair_mapping_commit_refused')
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='historical_terminal_bindings'").fetchone():
            require(not db.execute('SELECT 1 FROM historical_terminal_bindings LIMIT 1').fetchone(),
                    'reviewed_import_repair_history_commit_refused')
        original_shape={r[0]:[c[1] for c in db.execute('PRAGMA table_info('+quote(r[0])+')')]
                        for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='historical_terminal_bindings' ORDER BY name")}
        original_snapshot={t:table_digest(db,t,cols) for t,cols in original_shape.items()}
    finally:db.close()
    raw=read_private(folder/'checkpoint.json')
    if artifact.exists():require(read_private(artifact)==raw,'reviewed_import_repair_checkpoint_changed')
    else:create_private(artifact,raw)
    if fault is not None:fault('repair_original_checkpoint_saved')
    backup=folder/'source-before-reviewed-import-repair.sqlite'
    if not backup.exists():backup_database(target,backup)
    saved=ro_db(backup)
    try:
        saved.execute('BEGIN')
        require(saved.execute('PRAGMA quick_check').fetchone()[0]=='ok' and
                {t:table_digest(saved,t,cols) for t,cols in original_shape.items()}==original_snapshot,
                'reviewed_import_repair_source_backup_failed')
    finally:saved.close()
    if fault is not None:fault('repair_source_backup_verified')
    # Only the source helper's reviewed explicit operator API may add its table.
    db=sqlite3.connect(private_file(target).as_uri()+'?mode=rw',uri=True,isolation_level=None)
    db.row_factory=sqlite3.Row;db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
    try:source.extend_import_schema(db,cfg['source_namespace'],cp['source_canonical_digest'])
    finally:db.close()
    if fault is not None:fault('repair_schema_extension_committed')
    cp.update(source_helper_sha=source_sha,repair_from_source_sha=previous_sha,
              repair_checkpoint_sha256=sha(raw),repair_source_backup_sha256=artifact_digest(backup),
              repair_original_source_tables=original_snapshot)
    update_checkpoint(folder,cp)
    if fault is not None:fault('repair_checkpoint_committed')
def preserved(cfg_path,cfg,folder,cp,source_original=True):
    require(sha(read_private(cfg_path))==cp['config_sha256'],'canonical_config_changed')
    for key in ('preferences','writer'):
        require(sha(read_private(cp['original_paths'][key]))==cp['file_sha256'][key],'original_'+key+'_changed')
    if source_original:
        require(sha(read_private(cp['original_paths']['source']))==cp['file_sha256']['source'],'original_source_changed')
    db=ro_db(cfg['database'])
    try:
        db.execute('BEGIN');quiet(db)
        shape={t:v['columns'] for t,v in cp['ledger'].items()}
        require(ledger_snapshot(db,shape)==cp['ledger'],'original_ledger_fields_changed')
    finally:db.close()

def run(action,config,folder,ack=False,preferences=None,writer=None,source_db=None,
        source_helper=None,source_sha=None,runtime=None,runtime_sha=None,fault=None,
        confirmed_kind=None,preference_sha=None,previous_source_sha=None):
    folder=private_dir(folder);config=private_file(config);original=read_private(config,65536);cfg=decode(original)
    require(isinstance(cfg,dict) and cfg.get('database'),'host_config_required')
    require(previous_source_sha is None or action=='import','reviewed_import_repair_action_required')
    with quiescent(cfg,ack):
        if action=='backup':
            require(cfg.get('source_json') and not cfg.get('source_db'),'legacy_source_binding_required')
            source=read_private(cfg['source_json']);document=decode(source)
            require(isinstance(document,dict),'source_object_required')
            values={'source':source,'preferences':read_private(preferences),'writer':read_private(writer),'config':original}
            for key,value in values.items():create_private(folder/(key+('.py' if key=='writer' else '.json')),value)
            backup_database(cfg['database'],folder/'ledger.sqlite')
            db=ro_db(cfg['database']);saved=ro_db(folder/'ledger.sqlite')
            try:
                db.execute('BEGIN');saved.execute('BEGIN');quiet(db)
                live=ledger_snapshot(db);require(live==ledger_snapshot(saved),'ledger_snapshot_changed')
            finally:db.close();saved.close()
            cp={'version':1,'stage':'backed_up','config_sha256':sha(original),'file_sha256':{k:sha(v) for k,v in values.items()},
                'original_paths':{'source':cfg['source_json'],'preferences':str(Path(preferences)),'writer':str(Path(writer))},
                'source_canonical_digest':sha(canonical(document).encode()),'source_field_counts':field_counts(document),'ledger':live}
            create_private(folder/'checkpoint.json',(canonical(cp)+'\n').encode())
            preserved(config,cfg,folder,cp)
            result={'status':'verified','action':action,'private_backup':True,'source_preferences_writer_config_preserved':True,'ledger_fields_equal':True}
        else:
            cp=checkpoint(folder)
            if cp['stage']=='cutover_prepared' and sha(original)==cp.get('new_config_sha256'):
                old_cfg=decode(read_private(folder/'config.json',65536));changed={'source_json','source_db','source_schema','source_journal'}
                require(cfg.get('source_schema')==2 and cfg.get('source_db')==cp.get('source_db') and not cfg.get('source_json'),'cutover_recovery_binding_failed')
                require({k:v for k,v in cfg.items() if k not in changed}=={k:v for k,v in old_cfg.items() if k not in changed},'cutover_recovery_unrelated_config_change')
                cp.update(stage='cutover',config_sha256=sha(original));update_checkpoint(folder,cp)
            if action not in ('fallback','export-current'):
                preserved(config,cfg,folder,cp)
            source=None
            if action in ('import','bind-preferences','verify','cutover','fallback','export-current'):
                source=load_module(source_helper,source_sha,'reviewed_source_migration')
            if action=='import':
                target=Path(source_db);private_dir(target.parent)
                require(target.is_absolute() and target!=Path(cfg['database']) and target!=Path(cfg.get('source_json','')),'distinct_new_source_required')
                document=decode(read_private(folder/'source.json'))
                require(sha(canonical(document).encode())==cp['source_canonical_digest'],'backup_source_digest_changed')
                if previous_source_sha is not None:
                    resume_reviewed_import(folder,cp,target,source,source_sha,previous_source_sha,document,cfg,fault)
                if cp.get('stage')=='import_prepared':
                    require(cp.get('source_db')==str(target) and cp.get('source_helper_sha')==source_sha,'import_recovery_checkpoint_mismatch')
                else:
                    require(cp['stage']=='backed_up' and not target.exists() and not target.is_symlink(),'fresh_import_target_required')
                    cp.update(stage='import_prepared',source_db=str(target),source_helper_sha=source_sha);update_checkpoint(folder,cp)
                if fault is not None:fault('before_source_creation')
                if not target.exists():
                    create_private(target,b'')
                    if fault is not None:fault('source_file_created')
                resume_empty=False
                if target.exists():
                    check=ro_db(target)
                    try:resume_empty=not check.execute('SELECT 1 FROM sqlite_master LIMIT 1').fetchone()
                    finally:check.close()
                db=source.connect(target,initialize=not target.exists(),resume_empty=resume_empty,namespace=cfg['source_namespace'])
                try:
                    if fault is not None:fault('source_schema_created')
                    require(source.meta(db,'namespace')==cfg['source_namespace'],'import_recovery_namespace_mismatch')
                    if source.meta(db,'legacy_imported')=='false':source.import_legacy(db,document,cfg['source_namespace'])
                    if fault is not None:fault('import_committed')
                    exported=source.export_legacy(db);meta=source.metadata(db)
                    require(exported==document and field_counts(exported)==cp['source_field_counts'],'lossless_source_field_comparison_failed')
                    require(meta['export_canonical_digest']==cp['source_canonical_digest'],'source_canonical_digest_failed')
                    require(not db.execute('PRAGMA foreign_key_check').fetchone(),'source_foreign_key_failed')
                    require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok','source_integrity_failed')
                    if source.meta(db,'generation')=='1':
                        ledger=ro_db(cfg['database'])
                        try:
                            ledger.execute('BEGIN');quiet(ledger)
                            bindings=source.import_ledger_bindings(db,ledger.execute('SELECT id,revision,source_ref,status FROM reminders ORDER BY id'))
                        finally:ledger.close()
                    else:
                        bindings={'imported_bindings':db.execute('SELECT count(*) FROM reminder_mappings').fetchone()[0]+
                                  db.execute('SELECT count(*) FROM historical_terminal_bindings').fetchone()[0]}
                    if fault is not None:fault('bindings_committed')
                    require(verify_import_bindings(source,db,cfg['database'])==bindings['imported_bindings'],'imported_binding_count_mismatch')
                    meta=source.metadata(db)
                finally:db.close()
                sync_dir(target.parent)
                cp.update(stage='imported',source_db=str(target),source_helper_sha=source_sha,import_generation=meta['generation'],imported_ledger_bindings=bindings['imported_bindings'])
                update_checkpoint(folder,cp);preserved(config,cfg,folder,cp)
                result={'status':'verified','action':action,'all_original_fields_equal':True,'field_counts_equal':True,'canonical_digest_equal':True,'namespace_equal':True,'source_changed':False}
            elif action=='bind-preferences':
                require(cp['stage'] in ('imported','ledger_prepared') and cp.get('source_helper_sha')==source_sha,'reviewed_import_required')
                require(isinstance(confirmed_kind,str) and 1<=len(confirmed_kind)<=80,'reviewed_confirmation_kind_required')
                path=Path(source_helper).with_name('preference_tools.py')
                helper=load_module(path,preference_sha,'reviewed_preference_binding')
                db=source.connect(cp['source_db'])
                try:
                    require(source.meta(db,'namespace')==cfg['source_namespace'],'preference_namespace_mismatch')
                    helper.configure(db,cp['original_paths']['preferences'],confirmed_kind)
                    binding={'path':cp['original_paths']['preferences'],'confirmed_kind':confirmed_kind}
                    require(decode(source.meta(db,'preferences_binding').encode())==binding,'preference_binding_mismatch')
                    meta=source.metadata(db)
                finally:db.close()
                cp.update(preference_helper_sha=preference_sha,preference_binding=binding,import_generation=meta['generation'])
                update_checkpoint(folder,cp);preserved(config,cfg,folder,cp)
                result={'status':'verified','action':action,'bounded_timezone_binding':True,'preferences_bytes_preserved':True,'private_values_exported':False}
            elif action=='prepare-ledger':
                require(cp.get('stage') in ('imported','ledger_prepared'),'verified_source_import_required')
                module=load_module(runtime,runtime_sha,'reviewed_storage_runtime');store=module.Store(cfg['database']);store.db.close()
                preserved(config,cfg,folder,cp)
                cp.update(stage='ledger_prepared',runtime_sha=runtime_sha);update_checkpoint(folder,cp)
                result={'status':'verified','action':action,'original_ledger_fields_equal':True,'admissions_created':0,'sends':0}
            elif action in ('verify','cutover'):
                require(cp.get('source_helper_sha')==source_sha and cp.get('source_db'),'reviewed_import_checkpoint_required')
                db=source.connect(cp['source_db'],readonly=True)
                try:
                    db.execute('BEGIN')
                    source.recognize_import_schema(db)
                    meta=source.metadata(db)
                    require(meta['namespace']==cfg['source_namespace'] and meta['generation']==cp['import_generation'],'source_namespace_or_revision_changed')
                    require(meta['export_canonical_digest']==cp['source_canonical_digest'],'source_comparison_failed')
                    require(field_counts(source.export_legacy(db))==cp['source_field_counts'],'source_field_count_failed')
                    require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok' and not db.execute('PRAGMA foreign_key_check').fetchone(),
                            'source_integrity_failed')
                    require(verify_import_bindings(source,db,cfg['database'])==cp['imported_ledger_bindings'],
                            'imported_bindings_preservation_failed')
                    if cp.get('preference_binding'):
                        require(decode(source.meta(db,'preferences_binding').encode())==cp['preference_binding'],'preference_binding_changed')
                finally:db.close()
                if action=='cutover':
                    require(cp.get('runtime_sha') and cp['stage'] in ('ledger_prepared','cutover_prepared','cutover'),'reviewed_ledger_preparation_required')
                    updated=dict(cfg);updated.pop('source_json',None)
                    updated.update(source_db=cp['source_db'],source_schema=2,source_journal=str(Path(cp['source_db']).with_suffix('.operations.sqlite')))
                    changed={'source_json','source_db','source_schema','source_journal'}
                    require({k:v for k,v in cfg.items() if k not in changed}=={k:v for k,v in updated.items() if k not in changed},'unrelated_config_change')
                    if cp['stage']!='cutover':
                        cp.update(stage='cutover_prepared',new_config_sha256=sha((canonical(updated)+'\n').encode()))
                        update_checkpoint(folder,cp)
                        replace_private(config,(canonical(updated)+'\n').encode(),cp['config_sha256'])
                        if fault is not None:fault('config_replaced')
                        cp.update(stage='cutover',config_sha256=sha(read_private(config)));update_checkpoint(folder,cp)
                result={'status':'verified','action':action,'source_fields_digest_equal':True,'original_ledger_fields_equal':True,'route_namespace_identity_preferences_preserved':True,'single_source_binding':True,'lifecycle_effects':False}
            elif action in ('export-current','fallback'):
                require(cfg.get('source_schema')==2 and cfg.get('source_db')==cp.get('source_db'),'current_indexed_binding_required')
                require(source_sha==cp['source_helper_sha'],'reviewed_source_helper_required')
                db=source.connect(cfg['source_db'],readonly=True)
                try:exported=source.export_legacy(db);complete_state=source.export_state(db);meta=source.metadata(db)
                finally:db.close()
                # Export is recovery evidence only; never becomes a second writer.
                # Source generation cannot identify changes made only to the
                # ledger/journal. Every invocation gets an immutable private
                # recovery generation; earlier/partial evidence stays retained.
                recovery=folder/('current-'+str(meta['generation'])+'-'+sha(canonical(exported).encode())[:16]+'-'+os.urandom(16).hex())
                recovery.mkdir(mode=0o700);private_dir(recovery)
                export_path=recovery/'current-export.json'
                preserve_json_artifact(export_path,exported)
                state_path=recovery/'current-complete-state.json'
                preserve_json_artifact(state_path,complete_state)
                stores=[(cfg['source_db'],'current-source.sqlite'),(cfg['database'],'current-ledger.sqlite')]
                if cfg.get('source_journal') and Path(cfg['source_journal']).exists():
                    stores.append((cfg['source_journal'],'current-operations.sqlite'))
                for original_db,name in stores:
                    target=recovery/name
                    if not target.exists():backup_database(original_db,target)
                    live=ro_db(original_db);saved=ro_db(target)
                    try:
                        live.execute('BEGIN');saved.execute('BEGIN')
                        # Source includes FTS shadow tables; snapshot comparison
                        # here is exact private operator evidence, not a tool dump.
                        tables={r[0]:[c[1] for c in live.execute('PRAGMA table_info('+quote(r[0])+')')]
                            for r in live.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
                        require(ledger_snapshot(live,tables)==ledger_snapshot(saved,tables),'current_backup_changed')
                    finally:live.close();saved.close()
                require(meta['namespace']==cfg['source_namespace'],'current_namespace_mismatch')
                if action=='fallback':
                    updated={**cfg,'contextual_enabled':False}
                    replace_private(config,(canonical(updated)+'\n').encode(),sha(original))
                result={'status':'verified','action':action,'current_source_and_ledger_retained':True,'new_writes_preserved':True,'private_current_export_and_consistent_backups':True,'legacy_writer_not_restored':True,'contextual_sends_disabled':action=='fallback'}
            else:raise Refusal('unknown_operator_action')
        return {**result,'model_calls':0,'transport_calls':0,'personal_records_exported_to_console':False}

def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=('backup','import','bind-preferences','prepare-ledger','verify','cutover','export-current','fallback'))
    p.add_argument('--config',required=True);p.add_argument('--checkpoint-dir',required=True)
    p.add_argument('--operator-quiescence-ack',action='store_true')
    p.add_argument('--preferences');p.add_argument('--writer');p.add_argument('--source-db')
    p.add_argument('--source-helper');p.add_argument('--source-sha');p.add_argument('--runtime');p.add_argument('--runtime-sha')
    p.add_argument('--confirmed-kind');p.add_argument('--preference-sha')
    p.add_argument('--resume-previous-source-sha')
    p.add_argument('--bundle-root',required=True);p.add_argument('--bundle-manifest',required=True);p.add_argument('--bundle-sha',required=True)
    a=p.parse_args()
    verify_bundle(a.bundle_root,a.bundle_manifest,a.bundle_sha)
    # Both loaded helpers must belong to this already verified whole package.
    for path in (a.source_helper,a.runtime):
        if path is not None:require(Path(path).parent==Path(a.bundle_root)/'scripts','module_outside_reviewed_bundle')
    result=run(a.action,a.config,a.checkpoint_dir,a.operator_quiescence_ack,a.preferences,a.writer,a.source_db,a.source_helper,a.source_sha,a.runtime,a.runtime_sha,
        confirmed_kind=a.confirmed_kind,preference_sha=a.preference_sha,previous_source_sha=a.resume_previous_source_sha)
    print(canonical(result))
if __name__=='__main__':
    try:main()
    except Refusal as error:print(canonical({'status':'refused','reason':str(error),'personal_records_exported_to_console':False}));raise SystemExit(1)
    except Exception:print(canonical({'status':'refused','reason':'operation_failed_check_private_checkpoint','personal_records_exported_to_console':False}));raise SystemExit(1)
