#!/usr/bin/env python3
"""Reviewed additive notes activation under fresh PA pause and scheduler stop.

Creates only private backups/schema/config binding; no service control, scheduling or sends.
"""
import argparse
import json
import sqlite3
from pathlib import Path
import sys

import note_embedding
import note_store as notes
import return_tools
import source_store as source
import storage_migration as m


def shape(db):
    return {r[0]:[c[1] for c in db.execute('PRAGMA table_info('+m.quote(r[0])+')')]
            for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")}


def snap(path,projection=None):
    db=m.ro_db(path)
    try:
        db.execute('BEGIN')
        return m.ledger_snapshot(db,projection or shape(db))
    finally:db.close()


def backup(config,folder,ack):
    config=m.private_file(config);folder=m.private_dir(folder);raw=m.read_private(config,65536);cfg=m.decode(raw)
    m.require(cfg.get('source_schema')==2 and cfg.get('source_db') and not cfg.get('source_json'),'schema2_binding_required')
    with m.quiescent(cfg,ack):
        m.create_private(folder/'config-before.json',raw)
        cp={'version':1,'source_namespace':cfg['source_namespace'],'config_sha256':m.sha(raw),'paths':{},'snapshots':{},'preferences':{},'stage':'backed_up'}
        for name,key in [('source','source_db'),('ledger','database'),('journal','source_journal')]:
            path=Path(cfg[key]);cp['paths'][name]=str(path)
            if not path.exists():
                m.require(name=='journal','required_store_missing');cp['snapshots'][name]=None;continue
            before=snap(path);target=folder/(name+'-before.sqlite');m.backup_database(path,target)
            m.require(snap(target)==before==snap(path),'fresh_sqlite_preservation_failed')
            cp['snapshots'][name]=before
        db=source.connect(cfg['source_db'],readonly=True)
        try:
            m.require(source.meta(db,'namespace')==cfg['source_namespace'],'source_namespace_mismatch')
            cp['source_state_digest']=source.digest(source.export_state(db))
            pref=db.execute("SELECT value FROM metadata WHERE key='preferences_binding'").fetchone()
            if pref:
                path=Path(json.loads(pref[0])['path']);body=m.read_private(path)
                m.create_private(folder/'preferences-before.json',body)
                cp['preferences']={'path':str(path),'sha256':m.sha(body)}
        finally:db.close()
        m.require(m.sha(m.read_private(config,65536))==cp['config_sha256'],'config_changed_during_backup')
        m.create_private(folder/'checkpoint.json',(m.canonical(cp)+'\n').encode())
    return {'status':'fresh_private_backup_complete','source_integrity':True,'ledger_integrity':True,'journal_preserved':True,'data_only':True}


def preserved(cfg,folder,cp,extended=False):
    for name in ('ledger','journal'):
        original=cp['snapshots'][name];path=Path(cp['paths'][name])
        if original is None:m.require(not path.exists(),'unpaused_journal_creation');continue
        m.require(snap(path)==original,'current_'+name+'_changed')
    if cp['preferences']:
        m.require(m.sha(m.read_private(cp['preferences']['path']))==cp['preferences']['sha256'],'current_preferences_changed')
    db=m.ro_db(cp['paths']['source'])
    try:
        db.execute('BEGIN');current=m.ledger_snapshot(db,{k:v['columns'] for k,v in cp['snapshots']['source'].items()})
        for table,before in cp['snapshots']['source'].items():
            if table=='metadata' and extended:
                original=m.ro_db(folder/'source-before.sqlite')
                try:
                    expected=dict(original.execute('SELECT key,value FROM metadata'))
                    actual=dict(db.execute('SELECT key,value FROM metadata'))
                    m.require(all(actual.get(k)==v for k,v in expected.items()) and
                              {'notes_schema','notes_generation'}<=set(actual)-set(expected)<={'notes_schema','notes_generation','return_policy'},'original_metadata_changed')
                finally:original.close()
            else:m.require(current[table]==before,'original_source_table_changed')
        m.require(not db.execute('PRAGMA foreign_key_check').fetchone(),'source_foreign_key_failed')
    finally:db.close()


def extend(config,folder,ack,settings,authority_refs):
    config=m.private_file(config);folder=m.private_dir(folder);cp=m.decode(m.read_private(folder/'checkpoint.json'))
    saved=m.decode(m.read_private(folder/'config-before.json'));raw=m.read_private(config,65536);cfg=m.decode(raw)
    desired=saved|{'note_embedding':settings}
    # Validate offline pinned artifacts with a synthetic phrase; never index a real record during activation.
    embed=note_embedding.binding(desired);embed('query',['Synthetic private index health check'])
    with m.quiescent(cfg,ack):
        m.require(cfg==saved or cfg==desired,'unexpected_current_host_config')
        m.require(cfg['source_namespace']==cp['source_namespace'],'namespace_changed')
        db=source.connect(cfg['source_db'])
        try:
            already=bool(db.execute("SELECT 1 FROM metadata WHERE key='notes_schema'").fetchone())
            preserved(cfg,folder,cp,already)
            if not already:notes.extend(db,cp['source_namespace'],cp['source_state_digest'])
            else:notes.check(db)
            return_tools.configure(db,authority_refs)
            m.require(all(db.execute('SELECT count(*) FROM '+t).fetchone()[0]==0 for t in notes.TABLES),'pause_violated_new_note_writes')
        finally:db.close()
        # Original unrelated config values remain exactly equal; only offline binding added.
        if cfg!=desired:m.replace_private(config,(m.canonical(desired)+'\n').encode(),m.sha(raw))
        preserved(desired,folder,cp,True)
    return {'status':'notes_extension_bound','notes_schema':notes.VERSION,'original_source_preserved':True,
            'ledger_preserved':True,'journal_preserved':True,'preferences_preserved':True,'data_only':True}


def verify(config,folder,ack):
    cfg=m.decode(m.read_private(config,65536));cp=m.decode(m.read_private(Path(folder)/'checkpoint.json'))
    saved=m.decode(m.read_private(Path(folder)/'config-before.json'))
    with m.quiescent(cfg,ack):
        m.require({k:v for k,v in cfg.items() if k!='note_embedding'}==saved,'unrelated_host_config_changed')
        preserved(cfg,Path(folder),cp,True)
        db=source.connect(cfg['source_db'],readonly=True)
        try:notes.check(db);m.require(return_tools.policy(db) is not None,'standing_policy_missing')
        finally:db.close()
    return {'status':'paused_activation_verified','original_source_preserved':True,'ledger_preserved':True,
            'journal_preserved':True,'preferences_preserved':True,'source_binding_preserved':True,'data_only':True}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['backup','extend','verify']);p.add_argument('--config',required=True)
    p.add_argument('--checkpoint-dir',required=True);p.add_argument('--operator-quiescence-ack',action='store_true')
    p.add_argument('--bundle-root',required=True);p.add_argument('--bundle-manifest',required=True);p.add_argument('--bundle-sha',required=True)
    a=p.parse_args()
    try:
        m.verify_bundle(a.bundle_root,a.bundle_manifest,a.bundle_sha)
        m.require(Path(a.bundle_root)/'scripts/notes_activation.py'==Path(__file__).absolute(),'operator_script_bundle_mismatch')
        if a.action=='backup':result=backup(a.config,Path(a.checkpoint_dir),a.operator_quiescence_ack)
        elif a.action=='verify':result=verify(a.config,Path(a.checkpoint_dir),a.operator_quiescence_ack)
        else:
            raw=sys.stdin.buffer.read(8193);m.require(len(raw)<=8192,'operator_binding_input_overflow');data=m.decode(raw)
            m.require(set(data)=={'embedding','authority_refs'},'explicit_private_operator_binding_required')
            result=extend(a.config,Path(a.checkpoint_dir),a.operator_quiescence_ack,data['embedding'],data['authority_refs'])
        print(m.canonical(result));return 0
    except (ValueError,OSError,KeyError,TypeError,sqlite3.Error):
        print(m.canonical({'status':'activation_held','data_only':True}));return 1


if __name__=='__main__':sys.exit(main())
