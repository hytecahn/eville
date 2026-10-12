"""External freshness check; schedule independently of HA and the weekly AI job."""
import argparse
import json
from pathlib import Path
import sqlite3
import sys
import time


def check(database, node, now, threshold=180):
    db = sqlite3.connect('file:' + str(Path(database).resolve()) + '?mode=ro', uri=True)
    try:
        row = db.execute('SELECT max(collected) FROM batches WHERE node=?', (node,)).fetchone()
    finally:
        db.close()
    age = None if row[0] is None else now - row[0]
    return {'node': node, 'collector_age_seconds': age,
            'status': 'unknown' if age is None else ('stale' if age > threshold else 'receiving')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', required=True)
    parser.add_argument('--node', default='home')
    args = parser.parse_args()
    try:
        result = check(args.database, args.node, time.time())
    except sqlite3.Error:
        result = {'node': args.node, 'status': 'unknown', 'reason': 'database unavailable'}
    print(json.dumps(result))
    sys.exit(0 if result['status'] == 'receiving' else 2)
