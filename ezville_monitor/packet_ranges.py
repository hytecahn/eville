"""Bounded passive five-minute evidence query, excluding raw frames and log text."""
import json
import math
import re
import sqlite3
from packet_labels import classify


def read(database, node, start, end, now):
    if (type(start) not in (int, float) or type(end) not in (int, float)
            or not math.isfinite(start) or not math.isfinite(end)
            or not now - 90 * 86400 <= start < end <= now or end - start > 3600):
        raise ValueError('request a past window of at most one hour, within 90 days')
    profiles, markers, beats, windows = [], [], [], []
    gaps = malformed = 0
    truncated = False
    budget = 0
    with sqlite3.connect('file:' + str(database) + '?mode=ro', uri=True) as db:
        rows = db.execute('SELECT collected,body FROM batches WHERE node=? AND collected>=? AND collected<=? ORDER BY collected LIMIT 2001',
                          (node, start - 360, end + 90))
        for index, (collected, body) in enumerate(rows):
            if index >= 2000:
                truncated = True
                break
            for event in json.loads(body)['events']:
                if event['kind'] == 'collector_heartbeat' and start <= collected < end:
                    beats.append(collected)
                    gaps += not event.get('source_exists', False)
                elif event['kind'] == 'gap' and start <= collected < end:
                    gaps += 1
                elif event['kind'] == 'log':
                    line = event.get('text', '')
                    try:
                        if '[PACKET_WINDOW] ' in line:
                            item = json.loads(line.split('[PACKET_WINDOW] ', 1)[1])
                            if start <= item['end'] <= end + 90:
                                windows.append(min(end, item['end']))
                        elif '[PACKET_MARK] ' in line:
                            item = json.loads(line.split('[PACKET_MARK] ', 1)[1])
                            if start <= item['at'] < end:
                                if len(markers) < 20:
                                    markers.append({'at': item['at'], 'label': item['label'] if item['label'] in ('bell', 'door_open', 'car_entry', 'other') else 'other'})
                                else:
                                    truncated = True
                        elif '[PACKET_PROFILE] ' in line:
                            item = json.loads(line.split('[PACKET_PROFILE] ', 1)[1])
                            if item['first'] >= end or item['last'] < start:
                                continue
                            if not re.fullmatch(r'[0-9A-F]{2}(?::[0-9A-F]{2}){3}', item['signature']):
                                raise ValueError('invalid signature')
                            count = item['count']
                            variants = item.get('variants', {})
                            if (type(count) is not int or count < 0 or not isinstance(variants, dict) or len(variants) > 8
                                    or any(not re.fullmatch(r'[0-9a-f]{32}', key) or type(value) is not int or value < 0 for key, value in variants.items())):
                                raise ValueError('invalid count/variant')
                            if (any(type(item[key]) not in (int, float) or not math.isfinite(item[key]) for key in ('first', 'last'))
                                    or item['first'] > item['last']):
                                raise ValueError('invalid event time')
                            bucket = item.get('bucket_start', item['first'])
                            other = item.get('other_variant_count', 0)
                            changes = item.get('variant_changes', 0)
                            if (type(bucket) not in (int, float) or not math.isfinite(bucket)
                                    or type(other) is not int or other < 0
                                    or type(changes) is not int or changes < 0):
                                raise ValueError('invalid bucket statistics')
                            record = classify({'signature': item['signature'], 'known': bool(item['known']),
                                               'count': count, 'first': item['first'], 'last': item['last']})
                            record.update(bucket_start=bucket,
                                          tracked_variants=variants, other_variant_count=other,
                                          variant_changes=changes,
                                          variants_available='variants' in item,
                                          partially_overlapping=item['first'] < start or item['last'] >= end)
                            length = len(json.dumps(record).encode())
                            if len(profiles) >= 1024 or budget + length > 190000:
                                truncated = True
                                continue
                            profiles.append(record)
                            budget += length
                    except (ValueError, TypeError, KeyError):
                        malformed += 1
    checkpoints = [start] + sorted(set(beats)) + [end]
    gap = max(b - a for a, b in zip(checkpoints, checkpoints[1:]))
    packet_checkpoints = [start] + sorted(set(windows)) + [end]
    packet_gap = max(b - a for a, b in zip(packet_checkpoints, packet_checkpoints[1:]))
    return {'node': node, 'window_start': start, 'window_end': end, 'profiles': profiles,
            'markers': markers, 'truncated': truncated, 'malformed_records': malformed,
            'source_missing_or_gaps': gaps, 'collector_max_gap_seconds': round(gap),
            'packet_profile_max_gap_seconds': round(packet_gap),
            'observation_complete': bool(beats and windows) and gap <= 180 and packet_gap <= 360 and not (gaps or malformed or truncated),
            'limitations': ['Counts refer to full profile buckets; partially overlapping buckets are not exact window counts.',
                           'Fingerprints are bounded samples, not raw payloads or the complete set of variants.',
                           'A temporal association does not verify car entry; the notification may use a different bus.']}
