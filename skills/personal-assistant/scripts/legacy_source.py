#!/usr/bin/env python3
"""Read-only bounded recovery of an existing private legacy JSON source."""
import hashlib
import json
import os
from pathlib import Path
import stat



def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, 'duplicate JSON fields rejected')
        result[key] = value
    return result


def signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def read_private_json(path):
    """One stable bounded document. No initialization, retry loop, or private output."""
    path = Path(path)
    require(path.is_absolute(), 'source JSON path must be absolute')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1,
                'source JSON must be one regular file')
        require(before.st_uid == os.getuid() and stat.S_IMODE(before.st_mode) == 0o600,
                'source JSON requires private owner and mode 600')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read()
        after = os.fstat(fd)
        current = os.stat(path, follow_symlinks=False)
        require(signature(before) == signature(after) == signature(current),
                'source changed during read; hold for fresh recovery')
    finally:
        os.close(fd)
    document = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON rejected')))
    require(isinstance(document, dict), 'source JSON must be an object')
    return document


def field(record, name, limit=1000):
    value = record.get(name)
    require(isinstance(value, str) and bool(value.strip()) and (limit is None or len(value) <= limit),
            'unsupported legacy event field')
    return value


def event_record(record_source):
    from datetime import datetime, timedelta, timezone
    from zoneinfo import ZoneInfo
    record = {name: field(record_source, name, None if name in ('title', 'source') else 256 if name == 'source_wire_id' else 1000)
              for name in ('id', 'title', 'local_time', 'timezone', 'start_utc',
                           'source', 'source_wire_id', 'status')}
    require(record['status'] in ('active', 'cancelled'), 'unsupported legacy event status')
    effort, buffer = record_source.get('preparation_minutes'), record_source.get('buffer_minutes')
    require(all(type(v) is int and 0 <= v <= 525600 for v in (effort, buffer)),
            'confirmed legacy preparation effort/buffer required')
    local = datetime.fromisoformat(record['local_time'])
    require(local.tzinfo is None and 'T' in record['local_time'] and len(record['local_time']) >= 16,
            'legacy event requires explicit local date/time')
    require(record['start_utc'].endswith('Z') and 'T' in record['start_utc']
            and len(record['start_utc']) >= 17, 'legacy event requires explicit UTC date/time')
    start = datetime.fromisoformat(record['start_utc'][:-1] + '+00:00')
    require(start.tzinfo is not None and start.utcoffset() == timedelta(0),
            'legacy event requires aware UTC start')
    zone = ZoneInfo(record['timezone'])
    require(start.astimezone(zone).replace(tzinfo=None) == local,
            'legacy local time and UTC start disagree')
    record.update(local=record['local_time'], effort_minutes=effort, buffer_minutes=buffer,
                  prepare_utc=(start - timedelta(minutes=effort + buffer)).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z'))
    return record


def read_context(path, ref, namespace, max_nodes=None):
    """Verified legacy v1 event schema only; undefined task schema is held."""
    import uuid
    require(isinstance(namespace, str), 'private source namespace required')
    require(str(uuid.UUID(namespace)) == namespace, 'canonical private UUID namespace required')
    require(max_nodes is None or (type(max_nodes) is int and max_nodes > 0), 'legacy max_nodes hint must be positive')
    require(isinstance(ref, str) and ':' in ref, 'task/event source reference required')
    kind, key = ref.split(':', 1)
    require(kind in ('task', 'event') and bool(key.strip()) and len(key) <= 128,
            'task/event source reference required')
    document = read_private_json(path)
    require(type(document.get('schema_version')) is int and document['schema_version'] == 1,
            'unsupported legacy source schema')
    require(isinstance(document.get('tasks'), list) and isinstance(document.get('events'), list),
            'unsupported legacy source collections')
    # Empty live tasks have no agreed full schema or enum. Do not infer semantics
    # from another helper's few status comparisons, or invent record migrations.
    require(kind == 'event', 'legacy task schema unqualified; hold for explicit contract')
    events = document['events']
    selected = None
    seen = set()
    for candidate in events:
        require(isinstance(candidate, dict), 'unsupported legacy event record')
        ident = field(candidate, 'id', 128)
        require(ident not in seen, 'duplicate legacy event IDs rejected')
        seen.add(ident)
        if ident == key:
            selected = candidate
    require(selected is not None, 'unknown legacy event reference')
    record = event_record(selected)
    snapshot = {'namespace': namespace, 'ref': ref, 'kind': kind, 'root_id': key,
                'records': [record], 'dependent_ids': [], 'sendable': record['status'] == 'active'}
    # Covers complete selected source record, including future revision/provenance
    # fields, without exporting unknown fields or unrelated private state.
    encoded = json.dumps({'snapshot': snapshot, 'selected_source': selected},
                         sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')
    return {**snapshot, 'fingerprint': hashlib.sha256(encoded).hexdigest(), 'data_only': True}


def metadata_check(path, namespace):
    """Binding readiness booleans only. Never returns paths, IDs or record values."""
    import uuid
    require(isinstance(namespace, str) and str(uuid.UUID(namespace)) == namespace,
            'canonical private UUID namespace required')
    document = read_private_json(path)
    require(type(document.get('schema_version')) is int and document['schema_version'] == 1,
            'unsupported legacy source schema')
    require(all(isinstance(document.get(key), list) for key in ('tasks', 'events', 'reminders')),
            'unsupported legacy source collections')
    seen = set()
    for record in document['events']:
        require(isinstance(record, dict), 'unsupported legacy event record')
        normalized = event_record(record)
        require(normalized['id'] not in seen, 'duplicate legacy event IDs rejected')
        seen.add(normalized['id'])
    return {'status': 'verified', 'source_json_schema_version': 1,
            'event_schema_valid': True, 'private_owner_mode': True,
            'namespace_configured': True, 'read_only': True,
            'task_schema_qualified': False, 'source_migration_required': False,
            'preferences_exported': False}
