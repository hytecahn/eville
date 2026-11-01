"""Bounded passive five-minute evidence query, excluding raw frames and log text."""
import json
import math
import re
import sqlite3
import datetime as dt
from pathlib import Path
from packet_labels import classify

BUCKET_SECONDS = 300
MIN_BASELINE_BUCKETS = 6
MAX_SCAN_BATCHES = 30000


def read(database, node, start, end, now):
    if (type(start) not in (int, float) or type(end) not in (int, float)
            or not math.isfinite(start) or not math.isfinite(end)
            or not now - 90 * 86400 <= start < end <= now or end - start > 3600):
        raise ValueError('request a past window of at most one hour, within 90 days')
    profiles, markers, beats, windows = [], [], [], []
    gaps = malformed = 0
    truncated = False
    budget = 0
    # Profiles flush only after their five-minute bucket ends, and the collector
    # can ingest that line on its next poll. Include the final intersecting bucket.
    query_end = math.ceil(end / BUCKET_SECONDS) * BUCKET_SECONDS + 90
    with sqlite3.connect('file:' + str(Path(database).resolve()) + '?mode=ro', uri=True) as db:
        rows = db.execute('SELECT collected,body FROM batches WHERE node=? AND collected>=? AND collected<=? ORDER BY collected LIMIT 2001',
                          (node, start - 360, query_end))
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
                            if item['start'] < end and item['end'] > start:
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


def compare_action(database, node, action, at, now, radius=120):
    """Compare an operator-reported action time with nearby passive packet profiles.

    Returns only bounded signatures, counts and fingerprints, never raw frames.
    A single observation is a candidate, not a classification.
    """
    if action not in ('bell', 'door_open', 'elevator_arrival', 'elevator_location', 'other'):
        raise ValueError('unsupported action label')
    if (type(at) not in (int, float) or not math.isfinite(at)
            or type(radius) is not int or not 30 <= radius <= 300
            or at - radius < now - 90 * 86400 or at + radius > now + 60):
        raise ValueError('action time must be within 90 days, with radius 30–300 seconds')
    start, end = at - radius, at + radius
    evidence = read(database, node, start, end, now)
    candidates = []
    for item in evidence['profiles']:
        candidates.append({key: item.get(key) for key in (
            'signature', 'category', 'label', 'classification_basis', 'known',
            'count', 'first', 'last', 'bucket_start', 'tracked_variants',
            'other_variant_count', 'variant_changes', 'partially_overlapping')})
    candidates.sort(key=lambda item: (-item['count'], item['signature']))
    return {
        'node': node, 'reported_action': action, 'reported_at': at,
        'window_start': start, 'window_end': end,
        'candidate_profiles': candidates[:50], 'candidate_total': len(candidates),
        'observation_complete': evidence['observation_complete'],
        'collector_max_gap_seconds': evidence['collector_max_gap_seconds'],
        'packet_profile_max_gap_seconds': evidence['packet_profile_max_gap_seconds'],
        'source_missing_or_gaps': evidence['source_missing_or_gaps'],
        'truncated': evidence['truncated'], 'malformed_records': evidence['malformed_records'],
        'limitations': evidence['limitations'] + [
            'Operator-reported action times and temporal proximity are not proof of packet meaning.',
            'Five-minute profiles may overlap this window; counts are not exact event counts.',
            'Compare repeated action and control windows before assigning a packet meaning.']}


def discover_candidates(database, node, now, hours=24, include_samples=False):
    """Find passive anomalies against neighboring complete 5-minute buckets.

    A candidate requires a well-covered pair of adjacent profiles: one bucket
    with a change/new signature and one local control bucket. Never treats
    periodic high-volume signatures alone as events.
    """
    if type(hours) is not int or not 6 <= hours <= 24:
        raise ValueError('candidate window must be 6–24 hours')
    end = int(now // BUCKET_SECONDS) * BUCKET_SECONDS
    start = end - hours * 3600
    history_start = start - 7 * 86400
    by_bucket = {}
    window_meta, window_identity, overflow_counts = {}, {}, {}
    bad_buckets, beats, gaps, malformed, truncated = set(), [], 0, 0, False
    active_window_bucket = None
    with sqlite3.connect('file:' + str(database) + '?mode=ro', uri=True) as db:
        rows = db.execute(
            'SELECT collected,body FROM batches WHERE node=? AND collected>=? AND collected<? '
            'ORDER BY collected DESC LIMIT ?',
            (node, history_start, end + BUCKET_SECONDS + 90, MAX_SCAN_BATCHES + 1)).fetchall()
        matching_rows_seen = len(rows)
        if len(rows) > MAX_SCAN_BATCHES:
            truncated = True
            rows = rows[:MAX_SCAN_BATCHES]
        # Prefer recent evidence if the defensive bound is reached, then process chronologically.
        rows.reverse()
        for collected, body in rows:
            try:
                events = json.loads(body)['events']
            except (ValueError, TypeError, KeyError):
                malformed += 1
                continue
            for event in events:
                if event.get('kind') == 'collector_heartbeat':
                    if start <= collected < end:
                        beats.append(collected)
                        gaps += not event.get('source_exists', False)
                elif event.get('kind') == 'gap':
                    gaps += start <= collected < end
                elif event.get('kind') != 'log':
                    continue
                line = event.get('text', '')
                record_bucket = None
                try:
                    if '[PACKET_WINDOW] ' in line:
                        item = json.loads(line.split('[PACKET_WINDOW] ', 1)[1])
                        if (type(item.get('start')) not in (int, float) or type(item.get('end')) not in (int, float)
                                or not math.isfinite(item['start']) or not math.isfinite(item['end'])):
                            raise ValueError('bad window')
                        bucket = int(item['start'] // BUCKET_SECONDS) * BUCKET_SECONDS
                        record_bucket = bucket
                        active_window_bucket = bucket
                        duration = item['end'] - item['start']
                        total, signatures = item.get('total'), item.get('signatures')
                        if (type(total) is not int or total < 0 or type(signatures) is not int
                                or not 0 <= signatures <= 256):
                            raise ValueError('bad window counts')
                        identity = (item['start'], item['end'], total, signatures)
                        if not BUCKET_SECONDS - 60 <= duration <= BUCKET_SECONDS + 60:
                            bad_buckets.add(bucket)
                        elif bucket in window_identity and window_identity[bucket] != identity:
                            bad_buckets.add(bucket)
                            malformed += 1
                        else:
                            window_identity[bucket] = identity
                            window_meta[bucket] = {'total': total, 'signatures': signatures,
                                                   'start': item['start'], 'end': item['end']}
                    elif '[PACKET_OVERFLOW]' in line:
                        amount = int(line.split('[PACKET_OVERFLOW]', 1)[1].strip())
                        if amount < 0 or active_window_bucket is None:
                            raise ValueError('bad overflow record')
                        overflow_counts[active_window_bucket] = overflow_counts.get(active_window_bucket, 0) + amount
                    elif '[PACKET_PROFILE] ' in line:
                        item = json.loads(line.split('[PACKET_PROFILE] ', 1)[1])
                        bucket = item.get('bucket_start')
                        if type(bucket) not in (int, float) or not math.isfinite(bucket):
                            continue
                        bucket = int(bucket // BUCKET_SECONDS) * BUCKET_SECONDS
                        record_bucket = bucket
                        signature = item.get('signature')
                        if not re.fullmatch(r'[0-9A-F]{2}(?::[0-9A-F]{2}){3}', signature or ''):
                            raise ValueError('bad signature')
                        count, changes = item.get('count'), item.get('variant_changes', 0)
                        other_variants = item.get('other_variant_count', 0)
                        variants = item.get('variants', {})
                        if (type(count) is not int or count < 0 or type(changes) is not int or changes < 0
                                or type(other_variants) is not int or other_variants < 0
                                or not isinstance(variants, dict) or len(variants) > 8
                                or any(not re.fullmatch(r'[0-9a-f]{32}', key) or type(value) is not int or value < 0
                                       for key, value in variants.items())):
                            raise ValueError('bad profile values')
                        samples = item.get('samples', [])
                        if not isinstance(samples, list) or len(samples) > 3 or any(
                                not isinstance(sample, str) or len(sample) > 512
                                or not re.fullmatch(r'(?:[0-9A-F]{2})+', sample) for sample in samples):
                            raise ValueError('invalid raw samples')
                        record_identity = json.dumps(item, sort_keys=True, separators=(',', ':'))
                        bucket_profiles = by_bucket.setdefault(bucket, {})
                        existing = bucket_profiles.get(signature)
                        if existing is not None:
                            if existing['record_identity'] != record_identity:
                                malformed += 1
                            continue
                        bucket_profiles[signature] = {
                            'count': count, 'changes': changes, 'variants': set(variants),
                            'other_variants': other_variants,
                            'known': bool(item.get('known')), 'samples': list(samples),
                            'record_identity': record_identity}
                except (ValueError, TypeError, KeyError):
                    malformed += 1
                    if record_bucket is not None:
                        bad_buckets.add(record_bucket)

    complete_windows = set()
    observed_window_buckets = set(window_meta) | set(by_bucket) | bad_buckets | set(overflow_counts)
    overflowed_windows = {bucket for bucket, amount in overflow_counts.items() if amount > 0}
    for bucket, meta in window_meta.items():
        profiles = by_bucket.get(bucket, {})
        actual_total = sum(item['count'] for item in profiles.values()) + overflow_counts.get(bucket, 0)
        if (bucket not in bad_buckets and overflow_counts.get(bucket, 0) == 0
                and len(profiles) == meta['signatures'] and actual_total == meta['total']):
            complete_windows.add(bucket)
    incomplete_profile_windows = len(observed_window_buckets - complete_windows)
    complete_buckets = sorted(bucket for bucket in complete_windows if start <= bucket < end)
    missing = []
    for bucket in range(start, end, BUCKET_SECONDS):
        if bucket not in complete_windows:
            missing.append(bucket)
    checkpoints = [start] + sorted(set(beats)) + [end]
    heartbeat_gap = max((b - a for a, b in zip(checkpoints, checkpoints[1:])), default=end - start)
    coverage_ok = (len(complete_buckets) >= MIN_BASELINE_BUCKETS
                   and not gaps and not malformed and not truncated
                   and not missing and incomplete_profile_windows == 0
                   and bool(beats) and heartbeat_gap <= 180)
    candidates = []
    filtered = {'incomplete_observation_window': 0, 'incomplete_neighbor_windows': 0, 'insufficient_same_slot_baseline': 0,
                'no_anomaly': 0, 'fingerprint_cap_variation_suppressed': 0,
                'known_non_elevator_traffic': 0, 'scan_truncated': 0}
    for bucket in complete_buckets:
        current = by_bucket.get(bucket, {})
        previous, following = by_bucket.get(bucket - BUCKET_SECONDS, {}), by_bucket.get(bucket + BUCKET_SECONDS, {})
        if not coverage_ok:
            filtered['incomplete_observation_window'] += 1
            continue
        if truncated or bucket - BUCKET_SECONDS not in complete_windows or bucket + BUCKET_SECONDS not in complete_windows:
            filtered['scan_truncated' if truncated else 'incomplete_neighbor_windows'] += 1
            continue
        signatures = set(current) | set(previous) | set(following)
        for sig in signatures:
            cur, prev, nxt = current.get(sig), previous.get(sig), following.get(sig)
            local_control = max(prev['count'] if prev else 0, nxt['count'] if nxt else 0)
            current_count = cur['count'] if cur else 0
            changed = cur is not None and cur['changes'] > 0
            # New pattern or a count spike must contrast with both local neighbors.
            local = dt.datetime.fromtimestamp(bucket, dt.timezone(dt.timedelta(hours=9)))
            slot = (local.hour, local.minute)
            historical_counts, historical_presence = [], 0
            historical_variants, historical_changes = set(), []
            for day in range(1, 8):
                prior_bucket = bucket - day * 86400
                prior_local = dt.datetime.fromtimestamp(prior_bucket, dt.timezone(dt.timedelta(hours=9)))
                if (prior_local.hour, prior_local.minute) != slot or prior_bucket not in complete_windows:
                    continue
                historical_presence += 1
                prior = by_bucket.get(prior_bucket, {}).get(sig)
                historical_counts.append(prior['count'] if prior else 0)
                if prior:
                    historical_variants.update(prior['variants'])
                    historical_changes.append(prior['changes'])
            baseline = sorted(historical_counts)
            median_count = baseline[len(baseline) // 2] if baseline else 0
            change_baseline = sorted(historical_changes)
            median_changes = change_baseline[len(change_baseline) // 2] if change_baseline else 0
            if historical_presence < MIN_BASELINE_BUCKETS or truncated:
                filtered['insufficient_same_slot_baseline'] += 1
                continue
            is_new = cur is not None and historical_presence >= MIN_BASELINE_BUCKETS and not any(historical_counts)
            spike_floor = max(4, local_control * 4, median_count * 4, median_count + 3)
            spike = current_count >= spike_floor and current_count > local_control and current_count > median_count
            adjacent_variants = (prev['variants'] if prev else set()) | (nxt['variants'] if nxt else set())
            novel_variants = len((cur['variants'] if cur else set()) - historical_variants - adjacent_variants)
            cap_hit = bool(cur and cur['other_variants'])
            variation = (not cap_hit and
                         (novel_variants > 0 or (changed and cur['changes'] >= max(3, median_changes * 3 + 2))))
            if cap_hit and (novel_variants > 0 or changed):
                filtered['fingerprint_cap_variation_suppressed'] += 1
            if not (is_new or spike or variation):
                filtered['no_anomaly'] += 1
                continue
            score = (4 if is_new else 0) + (3 if spike else 0) + (2 if novel_variants else 0) + (1 if changed else 0)
            item = classify({'signature': sig, 'known': bool(cur and cur['known']), 'category': 'unknown'})
            if cur and cur.get('known') and not str(item.get('category', '')).startswith('elevator_'):
                filtered['known_non_elevator_traffic'] += 1
                continue
            candidate = {
                'bucket_start': window_meta[bucket]['start'], 'bucket_end': window_meta[bucket]['end'],
                'time_precision_seconds': round(window_meta[bucket]['end'] - window_meta[bucket]['start']),
                'signature': sig,
                'label': item.get('label', '미분류 패킷'), 'category': item.get('category', 'unknown'),
                'classification_basis': item.get('classification_basis', 'structural_profile_candidate'),
                'candidate_kind': 'new_signature' if is_new else 'local_rate_or_variant_change',
                'count': current_count, 'control_before': prev['count'] if prev else 0,
                'control_after': nxt['count'] if nxt else 0,
                'same_slot_baseline_buckets': historical_presence,
                'same_slot_baseline_days_observed': historical_presence,
                'baseline_days_configured': 7,
                'same_slot_baseline_median': median_count,
                'new_tracked_variants': novel_variants,
                'fingerprint_cap_hit': cap_hit,
                'variant_changes': cur['changes'] if cur else 0, 'score': score,
                'observation_complete': coverage_ok,
                'limitation': '5-minute profile association only; no exact action time or meaning established.'}
            if include_samples:
                candidate['raw_samples'] = list(cur['samples']) if cur else []
            candidates.append(candidate)
    candidates.sort(key=lambda item: (-item['score'], -item['bucket_start'], -item['count'], item['signature']))
    return {'node': node, 'window_start': start, 'window_end': end,
            'bucket_seconds': BUCKET_SECONDS, 'complete_profile_buckets': len(complete_buckets),
            'batches_scanned': len(rows), 'batch_scan_limit': MAX_SCAN_BATCHES,
            'matching_rows_detected_at_least': MAX_SCAN_BATCHES + 1 if truncated else matching_rows_seen,
            'older_rows_omitted_at_least': 1 if truncated else 0,
            'candidate_total': len(candidates), 'candidates': candidates[:50],
            'candidate_list_truncated': len(candidates) > 50,
            'candidate_suppressed_counts': filtered,
            'missing_profile_buckets': len(missing), 'source_missing_or_gaps': gaps,
            'collector_max_gap_seconds': round(heartbeat_gap),
            'malformed_records': malformed, 'truncated': truncated,
            'observation_complete': coverage_ok and not missing,
            'baseline_days_configured': 7,
            'minimum_baseline_days_required': MIN_BASELINE_BUCKETS,
            'incomplete_profile_windows': incomplete_profile_windows,
            'overflowed_profile_windows': len(overflowed_windows),
            'limitations': [
                'Candidate times are 5-minute bucket ranges, not exact action times.',
                'Candidate ranking uses adjacent controls and up to seven prior same-time buckets; sparse baseline lowers confidence.',
                'Steady high-volume polling is suppressed; bus traffic and repeated frames can still create candidates.',
                'Only up to eight SHA-256 fingerprints per signature and bucket are retained; fingerprints cannot reveal byte values or floor numbers.',
                'No candidate may mean an action is absent, silent, on another bus, uncollected, or hidden by missing coverage.']}
