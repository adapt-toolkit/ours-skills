#!/usr/bin/env python3
"""Prepare one agent's destination-free self-reminder config and user unit."""
import argparse
import json
import os
from pathlib import Path
import re
import sys

from self_reminders import load_config


def write_new_or_identical(path, content):
    if path.exists():
        if path.is_symlink() or path.read_text() != content:
            raise ValueError('existing setup differs; reconcile instead of overwriting')
        return
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as f:
        f.write(content)


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser()
    p.add_argument('--agent-state-dir', required=True)
    p.add_argument('--fleet-control-module', required=True)
    p.add_argument('--state-dir', required=True)
    args = p.parse_args()
    state = Path(args.state_dir)
    agent = Path(args.agent_state_dir)
    control = Path(args.fleet_control_module)
    if any(not x.is_absolute() for x in (state, agent, control)) or not control.is_file():
        raise ValueError('actual absolute agent, state and installed control paths required')
    desc = json.loads((agent / '.ours-bridge/descriptor.json').read_text())
    role, cid = desc['role'], desc['cid']
    if not re.fullmatch(r'[A-Za-z0-9_-]+', role) or not re.fullmatch(r'[A-Fa-f0-9]{64}', cid):
        raise ValueError('invalid supervisor binding')
    state.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = state.stat()
    if state.is_symlink() or info.st_uid != os.getuid() or (info.st_mode & 0o777) != 0o700:
        raise ValueError('private owner state directory mode700 required')
    cfg = state / 'self-reminders.json'
    content = json.dumps({'database': str(state / 'self-reminders.sqlite3'), 'agent': role,
                          'expected_agent_cid': cid, 'agent_state_dir': str(agent),
                          'fleet_control_module': str(control)}, indent=2) + '\n'
    write_new_or_identical(cfg, content)
    load_config(cfg)
    def quote(value):
        # systemd specifier and environment expansion, not shell interpolation.
        return json.dumps(str(value).replace('%', '%%').replace('$', '$$'))
    command = ' '.join(map(quote, [sys.executable, Path(__file__).with_name('self_reminders.py'), '--config', cfg, 'serve']))
    unit = state / ('self-reminders-' + role + '.service')
    write_new_or_identical(unit, '[Unit]\nDescription=Agent self-reminders: ' + role +
                          '\n\n[Service]\nType=simple\nUMask=0077\nExecStart=' + command +
                          '\nRestart=on-failure\nRestartSec=10\n\n[Install]\nWantedBy=default.target\n')
    print(json.dumps({'config': str(cfg), 'unit': str(unit), 'agent': role,
                      'service_enabled': False, 'outbound_destination_required': False}))


if __name__ == '__main__':
    main()
