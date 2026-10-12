from collections import Counter
import json
from pathlib import Path
import re
import sqlite3


def report(database, node, now, days=7):
    start = now - days * 86400
    db = sqlite3.connect('file:' + str(Path(database).resolve()) + '?mode=ro', uri=True)
    rows = db.execute('SELECT collected,body FROM batches WHERE node=? AND collected>=? AND collected<=? ORDER BY collected',
                      (node, start, now))
    beats, missing, counts, gaps, examples = [], 0, Counter(), [], []
    statuses = Counter()
    batch_count = 0
    latest_status = {}
    for collected, body in rows:
        batch_count += 1
        for event in json.loads(body)['events']:
            kind = event['kind']
            if kind == 'collector_heartbeat':
                beats.append(collected)
                latest_status = event.get('status', {})
                missing += not event.get('source_exists', False)
                for name, item in event.get('status', {}).items():
                    if isinstance(item, dict):
                        statuses[name + ':' + item.get('state', item.get('observation', 'unknown'))] += 1
            elif kind == 'gap':
                if len(gaps) < 30:
                    gaps.append(event['text'])
            elif kind == 'log':
                text = event['text']
                for category, pattern in [('watchdog', r'\[WATCHDOG\]'), ('error', r'ERROR|Traceback|Exception'),
                                           ('timeout', r'timeout|타임아웃'), ('health', r'\[HEALTH\]')]:
                    if re.search(pattern, text, re.I):
                        counts[category] += 1
                        if category != 'health' and len(examples) < 30:
                            examples.append(text)
    db.close()
    checkpoints = [start] + sorted(set(beats)) + [now]
    max_gap = max(b-a for a, b in zip(checkpoints, checkpoints[1:]))
    complete = bool(beats) and max_gap <= 180 and not missing and not gaps
    return {'node': node, 'window_start': start, 'window_end': now,
            'batch_count': batch_count, 'collector_max_gap_seconds': max_gap,
            'source_missing_samples': missing, 'explicit_gaps': gaps,
            'observation_complete': complete, 'counts': dict(counts), 'status_counts': dict(statuses), 'examples': examples,
            'latest_status': latest_status, 'limitations': ['Times describe collection, not necessarily event occurrence.',
                           'Collector heartbeat does not prove HA or device health.',
                           'No elevator commands are sent by this workflow.']}

