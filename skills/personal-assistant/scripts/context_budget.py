#!/usr/bin/env python3
"""Final-serialization accounting and call permissions; no local content-size caps."""
import json

# None explicitly means unbounded content size; these are not raised thresholds.
LIMITS = {name: (None, None) for name in ('source', 'memory', 'control')}
TOTAL = (None, None)
CALL_LIMITS = {'source': 3, 'memory': 1}
_encoder = None


class ContextOverflow(ValueError):
    def __init__(self, reason='context_overflow'):
        super().__init__(reason)


def compact_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def configure_encoder(encoder=None):
    """Operator/test injection only. Never download token data during module import."""
    global _encoder
    _encoder = encoder


def measure(value):
    raw = compact_json(value).encode('utf-8')
    if _encoder is None:
        # Every o200k BPE token has >=1 UTF-8 byte; this conservative upper bound
        # needs no runtime package/download and cannot underestimate that proxy.
        return {'tokens': len(raw), 'bytes': len(raw), 'meter': 'utf8_byte_upper_bound'}
    return {'tokens': len(_encoder.encode(raw.decode('utf-8'), disallowed_special=())),
            'bytes': len(raw), 'meter': 'o200k_base_proxy'}


def fits(value, slot='source'):
    if slot != 'total' and slot not in LIMITS:
        raise ValueError('invalid slot')
    measure(value)  # Preserve complete strict serialization validation.
    return True


def bounded_result(value, slot='source'):
    if not fits(value, slot):
        raise ContextOverflow()
    return value


def failure(reason='context_overflow'):
    if reason not in ('context_overflow', 'call_limit', 'context_ticket_required', 'source_unavailable',
                      'source_changed', 'source_query_overflow', 'invalid_request', 'unsupported_schema', 'budget_meter_changed'):
        reason = 'invalid_request'
    return {'data_only': True, 'status': reason, 'sendable': False, 'hold': True}


class TurnBudget:
    """Charge final outputs, including failures and repeated responses.

    Its state must be stored by a trusted event/host ticket owner. A model-supplied
    new turn ID or a fresh CLI object is NOT cross-process whole-turn enforcement.
    Intake uses one batched response; D2's runtime persists real-event counters.
    """
    def __init__(self, state=None):
        meter = measure({})['meter']
        self.state = {'version': 1, 'meter': meter, 'total': {'tokens': 0, 'bytes': 0},
                      'slots': {k: {'tokens': 0, 'bytes': 0, 'calls': 0} for k in LIMITS}}
        if state is not None:
            if not isinstance(state, dict) or state.get('version') != 1 or state.get('meter') != meter:
                raise ContextOverflow('budget_meter_changed')
            for name, limits in [('total', TOTAL)] + list(LIMITS.items()):
                row = state['total'] if name == 'total' else state['slots'][name]
                for field, maximum in zip(('tokens', 'bytes'), limits):
                    if type(row[field]) is not int or row[field] < 0:
                        raise ValueError('invalid trusted budget state')
                if name != 'total' and (type(row['calls']) is not int or row['calls'] < 0):
                    raise ValueError('invalid trusted budget state')
            self.state = json.loads(compact_json(state))

    def snapshot(self):
        return json.loads(compact_json(self.state))

    def remaining(self, slot='source'):
        if slot != 'total' and slot not in LIMITS:
            raise ValueError('invalid slot')
        return {k: None for k in ('tokens', 'bytes')}

    def admit(self, value, source_keys=(), memory_keys=(), whole_slot=None, source_fields=(), control_fields=()):
        """Caller constructs the final printed object, then commits returned state.

        Every key is charged. Explicit trusted partitions allow existing inspect
        shapes without dropping control/reminder/failure/cursor metadata.
        """
        if not isinstance(value, dict):
            raise ValueError('final tool response must be an object')
        if whole_slot is not None:
            if whole_slot not in LIMITS or source_fields or control_fields:
                raise ValueError('invalid slot')
            parts = {whole_slot: value}
        else:
            source_keys, memory_keys = set(source_keys), set(memory_keys)
            if source_keys & memory_keys:
                raise ValueError('overlapping budget partitions')
            nested = {}
            for parent, field in source_fields:
                if parent in source_keys | memory_keys:
                    raise ValueError('overlapping budget partitions')
                nested.setdefault(parent, set()).add(field)
            controls = {}
            for parent, field in control_fields:
                if parent not in source_keys or parent in nested:
                    raise ValueError('control field requires a source partition')
                controls.setdefault(parent, set()).add(field)
            parts = {name: {} for name in LIMITS}
            for key, item in value.items():
                if key in controls:
                    if not isinstance(item, dict):
                        raise ValueError('nested budget partition must be an object')
                    source = {k: v for k, v in item.items() if k not in controls[key]}
                    control = {k: v for k, v in item.items() if k in controls[key]}
                    if source or not item:
                        parts['source'][key] = source
                    if control:
                        parts['control'][key] = control
                    continue
                if key in nested:
                    if not isinstance(item, dict):
                        raise ValueError('nested budget partition must be an object')
                    source = {k: v for k, v in item.items() if k in nested[key]}
                    control = {k: v for k, v in item.items() if k not in nested[key]}
                    if source:
                        parts['source'][key] = source
                    if control or not item:
                        parts['control'][key] = control
                    continue
                slot = 'source' if key in source_keys else 'memory' if key in memory_keys else 'control'
                parts[slot][key] = item
            parts = {name: part for name, part in parts.items() if part}
        charged = {slot: measure(part) for slot, part in parts.items()}
        total = measure(value)
        for slot, m in charged.items():
            row = self.state['slots'][slot]
            if slot in CALL_LIMITS and row['calls'] >= CALL_LIMITS[slot]:
                raise ContextOverflow('call_limit')
        for slot, m in charged.items():
            row = self.state['slots'][slot]
            row['tokens'] += m['tokens']
            row['bytes'] += m['bytes']
            row['calls'] += 1
        for field in ('tokens', 'bytes'):
            self.state['total'][field] += total[field]
        return value

    def admit_failure(self, reason):
        value = failure(reason)
        m = measure(value)
        row = self.state['slots']['control']
        for field in ('tokens', 'bytes'):
            row[field] += m[field]
            self.state['total'][field] += m[field]
        row['calls'] += 1
        return value
