#!/usr/bin/env python3
"""Initialize empty private source/configuration files. No scheduling or messaging."""
import argparse
import json
import os
from pathlib import Path
from source_store import connect, run


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', required=True)
    args = parser.parse_args()
    path = Path(args.workspace)
    path.mkdir(parents=True, exist_ok=False, mode=0o700)
    db = connect(path / 'source.sqlite3', initialize=True)
    namespace = run(db, 'init', {})['namespace']
    db.close()
    template = Path(__file__).resolve().parent.parent / 'assets/activation.template.json'
    config = json.loads(template.read_text())
    config['namespace'] = namespace
    for name, value in (('activation.json', config), ('preferences.json', {}), ('event-reminders.json', {})):
        with (path / name).open('x') as stream:
            json.dump(value, stream, indent=2)
            stream.write('\n')
    print(json.dumps({'ok': True, 'namespace': namespace, 'runtime_enabled': False}))


if __name__ == '__main__':
    main()
