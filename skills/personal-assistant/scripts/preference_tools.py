#!/usr/bin/env python3
"""Read one confirmed timezone with provenance. Never expose whole preferences."""
from datetime import datetime,timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import json

from legacy_source import read_private_json
import source_store as source


def project_timezone(document, confirmed_kind):
    if not isinstance(document,dict) or not isinstance(confirmed_kind,str) or not 1<=len(confirmed_kind)<=80:
        raise ValueError('reviewed explicit timezone provenance contract required')
    zone=document.get('timezone');origin=document.get('timezone_source')
    if (not isinstance(zone,str) or not 1<=len(zone)<=128 or not isinstance(origin,dict)
            or origin.get('kind')!=confirmed_kind or not isinstance(origin.get('wire_id'),str)
            or not 1<=len(origin['wire_id'])<=256 or '\x00' in origin['wire_id']
            or not isinstance(origin.get('confirmed_at_utc'),str)
            or not origin['confirmed_at_utc'].endswith('Z')):
        raise ValueError('confirmed timezone or exact provenance missing')
    when=datetime.fromisoformat(origin['confirmed_at_utc'][:-1]+'+00:00')
    if 'T' not in origin['confirmed_at_utc'] or when.utcoffset()!=timedelta(0):
        raise ValueError('explicit timezone confirmation UTC instant required')
    ZoneInfo(zone)
    return {'timezone':zone,'timezone_source':{k:origin[k] for k in ('kind','wire_id','confirmed_at_utc')},
            'confirmed':True,'data_only':True}


def configure(db, path, confirmed_kind):
    """Operator verifies kind denotes explicit user confirmation; private only."""
    path=Path(path)
    project_timezone(read_private_json(path),confirmed_kind)
    with source.atomic(db,True):
        source.check_schema(db)
        value=source.compact_json({'path':str(path),'confirmed_kind':confirmed_kind})
        old=db.execute("SELECT value FROM metadata WHERE key='preferences_binding'").fetchone()
        if old is not None and old[0]!=value:
            raise ValueError('preference binding replacement requires reviewed operator migration')
        db.execute("INSERT OR IGNORE INTO metadata VALUES ('preferences_binding',?)",(value,))


def read(db, keys):
    if keys!=['timezone']:
        raise ValueError('one explicit timezone preference only')
    try:binding=json.loads(source.meta(db,'preferences_binding'))
    except ValueError:return {'status':'not_configured','data_only':True}
    return project_timezone(read_private_json(Path(binding['path'])),binding['confirmed_kind'])
