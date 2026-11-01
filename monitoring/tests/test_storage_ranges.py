import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ezville_monitor'))
import packet_ranges
import storage


class StorageRangeTests(unittest.TestCase):
    def add_profile_window(self, conn, node, bucket, signature, count, offset=0,
                           variant='a' * 32, changes=0):
        started = bucket + offset
        window = {'start': started, 'end': started + 300, 'total': count, 'signatures': 1}
        profile = {'signature': signature, 'known': False, 'count': count,
                   'first': started + 10, 'last': started + 280,
                   'bucket_start': started, 'variants': {variant: count},
                   'other_variant_count': 0, 'variant_changes': changes, 'samples': ['F799010203AA']}
        body = {'events': [
            {'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps(window)},
            {'kind': 'log', 'text': '[PACKET_PROFILE] ' + json.dumps(profile)},
            {'kind': 'collector_heartbeat', 'source_exists': True}]}
        conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', started + 301, json.dumps(body)))

    def add_incomplete_window(self, conn, bucket, with_overflow=False):
        marker = {'start': bucket, 'end': bucket + 300, 'total': 100, 'signatures': 3 if not with_overflow else 1}
        profile = {'signature': '99:01:02:00', 'known': False, 'count': 100 if not with_overflow else 5,
                   'first': bucket + 10, 'last': bucket + 280, 'bucket_start': bucket,
                   'variants': {'a' * 32: 5}, 'other_variant_count': 0, 'variant_changes': 0,
                   'samples': ['F799010203AA']}
        events = [{'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps(marker)},
                  {'kind': 'log', 'text': '[PACKET_PROFILE] ' + json.dumps(profile)}]
        if with_overflow:
            events.append({'kind': 'log', 'text': '[PACKET_OVERFLOW] 95'})
        conn.execute('INSERT INTO batches VALUES (?,?,?)',
                     ('home', bucket + 301, json.dumps({'events': events})))

    def add_conflicting_duplicate_profile(self, conn, bucket, signature, count):
        profile = {'signature': signature, 'known': False, 'count': count,
                   'first': bucket + 10, 'last': bucket + 280, 'bucket_start': bucket,
                   'variants': {'b' * 32: count}, 'other_variant_count': 0,
                   'variant_changes': 0, 'samples': ['F799010203BB']}
        event = {'kind': 'log', 'text': '[PACKET_PROFILE] ' + json.dumps(profile)}
        conn.execute('INSERT INTO batches VALUES (?,?,?)',
                     ('home', bucket + 302, json.dumps({'events': [event]})))

    def add_complete_current_coverage(self, conn, now, excluded=()):
        end = int(now // 300) * 300
        start = end - 24 * 3600
        excluded = set(excluded)
        for bucket in range(start, end, 300):
            if bucket in excluded:
                continue
            marker = {'start': bucket, 'end': bucket + 300, 'total': 0, 'signatures': 0}
            conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', bucket + 301, json.dumps({'events': [
                {'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps(marker)}]})))
        heartbeat = json.dumps({'events': [{'kind': 'collector_heartbeat', 'source_exists': True}]})
        conn.executemany('INSERT INTO batches VALUES (?,?,?)', (
            ('home', second, heartbeat) for second in range(start, end, 60)))

    def test_discovery_uses_same_slot_and_adjacent_baseline_not_steady_polling(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 100)
                for bucket in (target - 300, target, target + 300):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', 100)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertEqual(result['candidate_total'], 0)  # steady periodic high volume is normal
            self.assertTrue(result['observation_complete'])

    def test_regular_variant_only_candidates_are_grouped_below_local_and_rare_candidates(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            end = int(now // 300) * 300
            start = end - 24 * 3600
            target = start + 3600
            periodic_sig = '40:01:07:A5'
            stable_variant = 'f' * 32
            event_buckets = []
            current_buckets = list(range(start, end, 300))
            event_buckets = [bucket for bucket in current_buckets
                             if bucket >= target and (bucket - target) % 600 == 0
                             and bucket <= end - 600]
            spike_bucket = target - 1800
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, current_buckets)
                for bucket in current_buckets:
                    is_event = bucket in event_buckets
                    count = 20 if bucket == spike_bucket else 2 if is_event else 1
                    variant = (f'{bucket:032x}'[-32:] if is_event else stable_variant)
                    self.add_profile_window(conn, 'home', bucket, periodic_sig, count, variant=variant)
                for bucket in event_buckets:
                    for day in range(1, 8):
                        self.add_profile_window(conn, 'home', bucket - day * 86400,
                                                periodic_sig, 2, variant=stable_variant)
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', spike_bucket - day * 86400,
                                            periodic_sig, 2, variant=stable_variant)
                # A local count spike in the recurring signature remains primary.
            result = packet_ranges.discover_candidates(db, 'home', now)
            groups = [item for item in result['periodic_variant_groups']
                      if item['signature'] == periodic_sig]
            self.assertEqual(len(groups), 1)
            group = groups[0]
            self.assertEqual(group['candidate_kind'], 'periodic_variant_only')
            self.assertEqual(group['priority'], 'low')
            self.assertEqual(group['cadence_seconds'], 600)
            self.assertGreaterEqual(group['occurrence_count'], 5)
            self.assertEqual(group['count_median'], 2)
            self.assertEqual(group['control_before_median'], 1)
            self.assertEqual(group['control_after_median'], 1)
            self.assertEqual(group['same_slot_baseline_median'], 2)
            self.assertEqual(group['variant_changes_total'], 0)
            self.assertTrue(group['occurrence_examples_truncated'])
            primary_periodic = [item for item in result['candidates']
                                if item['signature'] == periodic_sig]
            self.assertEqual(len(primary_periodic), 1)
            self.assertEqual(primary_periodic[0]['bucket_start'], spike_bucket)
            self.assertEqual(result['candidate_occurrences_total'],
                             result['candidate_total'] + result['periodic_variant_occurrences_grouped'])

    def test_fewer_than_five_variant_only_occurrences_stay_in_primary_candidates(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            end = int(now // 300) * 300
            start = end - 24 * 3600
            targets = [end - 7200, end - 6600, end - 6000, end - 5400]
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                excluded = {bucket + delta for bucket in targets for delta in (-300, 0, 300)}
                self.add_complete_current_coverage(conn, now, excluded)
                for day in range(1, 8):
                    for bucket in targets:
                        self.add_profile_window(conn, 'home', bucket - day * 86400,
                                                '40:01:07:A5', 2, variant='f' * 32)
                for bucket in sorted(excluded):
                    is_target = bucket in targets
                    variant = f'{bucket:032x}'[-32:] if is_target else 'f' * 32
                    self.add_profile_window(conn, 'home', bucket, '40:01:07:A5',
                                            2 if is_target else 1, variant=variant)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertEqual(result['periodic_variant_group_total'], 0)
            self.assertEqual(len([item for item in result['candidates']
                                  if item['signature'] == '40:01:07:A5']), 4)

    def test_irregular_variant_only_candidates_are_not_mislabeled_periodic(self):
        starts = [0, 600, 1500, 2100, 3000]
        items = [{'signature': '40:01:07:A5', 'candidate_signal': 'variant_only',
                  'bucket_start': start, 'bucket_end': start + 300, 'count': 2,
                  'control_before': 1, 'control_after': 1, 'same_slot_baseline_median': 2,
                  'new_tracked_variants': 1, 'variant_changes': 0,
                  'label': '미분류 패킷', 'category': 'unknown'} for start in starts]
        primary, groups = packet_ranges.group_periodic_variant_candidates(items)
        self.assertEqual(len(primary), 5)
        self.assertEqual(groups, [])

    def test_off_phase_rare_variant_occurrence_stays_primary_inside_periodic_run(self):
        starts = sorted(list(range(0, 6600, 600)) + [3300])
        items = [{'signature': '40:01:07:A5', 'candidate_signal': 'variant_only',
                  'bucket_start': start, 'bucket_end': start + 300, 'count': 2,
                  'control_before': 1, 'control_after': 1, 'same_slot_baseline_median': 2,
                  'new_tracked_variants': 1, 'variant_changes': 0,
                  'label': '미분류 패킷', 'category': 'unknown'} for start in starts]
        primary, groups = packet_ranges.group_periodic_variant_candidates(items)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]['cadence_seconds'], 600)
        self.assertEqual(groups[0]['occurrence_count'], 11)
        self.assertEqual([item['bucket_start'] for item in primary], [3300])

    def test_equal_score_candidates_prefer_newer_window_over_old_high_volume(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            end = int(now // 300) * 300
            recent = end - 600
            older = end - 4 * 3600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (older - 300, older, older + 300,
                                                               recent - 300, recent, recent + 300))
                for target, amount in ((older, 1000), (recent, 20)):
                    for day in range(1, 8):
                        self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                    for bucket, count in ((target - 300, 5), (target, amount), (target + 300, 5)):
                        self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
            result = packet_ranges.discover_candidates(db, 'home', now)
            matching = [item for item in result['candidates'] if item['bucket_start'] in (older, recent)]
            self.assertEqual([item['bucket_start'] for item in matching], [recent, older])
            self.assertEqual(matching[0]['count'], 20)

    def test_discovery_surfaces_bucket_range_for_localized_rate_spike(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count,
                                            offset=7.25 if bucket == target else 0)
                self.add_profile_window(conn, 'home', target, '99:01:02:00', 100, offset=7.25)  # duplicate ingestion is idempotent
            result = packet_ranges.discover_candidates(db, 'home', now)
            candidate = next(item for item in result['candidates'] if item['bucket_start'] == target + 7.25)
            self.assertEqual(candidate['candidate_kind'], 'local_rate_or_variant_change')
            self.assertEqual(candidate['time_precision_seconds'], 300)
            self.assertEqual(candidate['control_before'], 5)
            self.assertEqual(candidate['control_after'], 5)
            self.assertEqual(candidate['count'], 100)
            self.assertEqual(candidate['same_slot_baseline_buckets'], 7)
            self.assertNotIn('F799010203AA', json.dumps(result))
            self.assertTrue(result['observation_complete'])
            local = packet_ranges.discover_candidates(db, 'home', now, include_samples=True)
            local_candidate = next(item for item in local['candidates'] if item['bucket_start'] == target + 7.25)
            self.assertEqual(local_candidate['raw_samples'], ['F799010203AA'])
            self.assertEqual(candidate['bucket_start'], target + 7.25)
            self.assertEqual(candidate['bucket_end'], target + 307.25)
            self.assertEqual(candidate['same_slot_baseline_days_observed'], 7)
            self.assertEqual(candidate['baseline_days_configured'], 7)

    def test_discovery_does_not_treat_marker_first_partial_profile_flush_as_zero_baseline(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target,))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                self.add_profile_window(conn, 'home', target - 300, '99:01:02:00', 5)
                self.add_incomplete_window(conn, target)  # marker promises 3 profiles, only first has arrived
                self.add_profile_window(conn, 'home', target + 300, '99:01:02:00', 5)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(result['observation_complete'])
            self.assertGreaterEqual(result['incomplete_profile_windows'], 1)
            self.assertGreaterEqual(result['missing_profile_buckets'], 1)
            self.assertFalse(any(item['bucket_start'] == target for item in result['candidates']))

    def test_discovery_suppresses_overflowed_flush_instead_of_treating_missing_profiles_as_zero(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target,))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                self.add_profile_window(conn, 'home', target - 300, '99:01:02:00', 5)
                self.add_incomplete_window(conn, target, with_overflow=True)
                self.add_profile_window(conn, 'home', target + 300, '99:01:02:00', 5)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(result['observation_complete'])
            self.assertEqual(result['overflowed_profile_windows'], 1)
            self.assertFalse(any(item['bucket_start'] == target for item in result['candidates']))

    def test_unrelated_incomplete_historical_bucket_does_not_suppress_complete_local_candidate(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    prior = target - day * 86400
                    if day == 7:
                        self.add_incomplete_window(conn, prior)
                    else:
                        self.add_profile_window(conn, 'home', prior, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
            result = packet_ranges.discover_candidates(db, 'home', now)
            candidate = next((item for item in result['candidates'] if item['bucket_start'] == target), None)
            self.assertIsNotNone(candidate, 'unrelated historical incompleteness should not block local evidence')
            self.assertEqual(candidate['same_slot_baseline_buckets'], 6)
            self.assertTrue(result['current_window_complete'])
            self.assertGreater(result['incomplete_profile_windows'], 0)

    def test_unrelated_current_gap_does_not_suppress_candidate_with_complete_local_evidence(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            unrelated = target - 6 * 3600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300, unrelated))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
            result = packet_ranges.discover_candidates(db, 'home', now)
            candidate = next((item for item in result['candidates'] if item['bucket_start'] == target), None)
            self.assertIsNotNone(candidate)
            self.assertFalse(result['current_window_complete'])
            self.assertFalse(result['observation_complete'])
            self.assertTrue(candidate['candidate_evidence_complete'])
            self.assertEqual(result['missing_profile_buckets'], 1)

    def test_sustained_new_signature_is_consolidated_as_one_episode(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            signature = '99:01:02:00'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for bucket in (target - 300, target, target + 300):
                    for day in range(1, 8):
                        self.add_profile_window(conn, 'home', bucket - day * 86400, signature, 0)
                for bucket in (target - 300, target, target + 300):
                    self.add_profile_window(conn, 'home', bucket, signature, 100)
            result = packet_ranges.discover_candidates(db, 'home', now)
            sustained = [item for item in result['candidates'] if item['signature'] == signature]
            self.assertEqual(len(sustained), 1)
            self.assertEqual(sustained[0]['candidate_kind'], 'sustained_new_signature_episode')
            self.assertEqual(sustained[0]['episode_bucket_count'], 3)
            self.assertEqual(sustained[0]['episode_bucket_start'], target - 300)
            self.assertEqual(sustained[0]['episode_bucket_end'], target + 600)

    def test_isolated_new_signature_remains_a_single_bucket_candidate(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            signature = '99:01:02:00'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target,))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, signature, 0)
                self.add_profile_window(conn, 'home', target, signature, 10)
            result = packet_ranges.discover_candidates(db, 'home', now)
            selected = [item for item in result['candidates'] if item['signature'] == signature]
            self.assertEqual(len(selected), 1)
            self.assertEqual(selected[0]['candidate_kind'], 'new_signature')
            self.assertNotIn('episode_bucket_count', selected[0])

    def test_conflicting_duplicate_current_profile_invalidates_only_its_bucket(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
                self.add_conflicting_duplicate_profile(conn, target, '99:01:02:00', 1)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertEqual(result['malformed_records'], 1)
            self.assertFalse(any(item['bucket_start'] == target for item in result['candidates']))
            self.assertGreaterEqual(result['current_incomplete_profile_windows'], 1)

    def test_conflicting_duplicate_adjacent_profile_blocks_candidate_using_it_as_control(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
                self.add_conflicting_duplicate_profile(conn, target - 300, '99:01:02:00', 4)
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(any(item['bucket_start'] == target for item in result['candidates']))
            self.assertGreater(result['candidate_suppressed_counts']['incomplete_neighbor_windows'], 0)

    def test_conflicting_duplicate_historical_profile_is_excluded_from_baseline(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            target = int(now // 300) * 300 - 600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                self.add_complete_current_coverage(conn, now, (target - 300, target, target + 300))
                for day in range(1, 8):
                    self.add_profile_window(conn, 'home', target - day * 86400, '99:01:02:00', 5)
                for bucket, count in ((target - 300, 5), (target, 100), (target + 300, 5)):
                    self.add_profile_window(conn, 'home', bucket, '99:01:02:00', count)
                self.add_conflicting_duplicate_profile(conn, target - 7 * 86400, '99:01:02:00', 4)
            result = packet_ranges.discover_candidates(db, 'home', now)
            candidate = next(item for item in result['candidates'] if item['bucket_start'] == target)
            self.assertEqual(result['malformed_records'], 1)
            self.assertEqual(candidate['same_slot_baseline_buckets'], 6)
            self.assertTrue(candidate['candidate_evidence_complete'])

    def test_zero_profiles_and_packet_overflow_make_reported_window_incomplete(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            end = int(now // 300) * 300
            start = end - 24 * 3600
            target = start + 12 * 3600
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                for second in range(start, end, 60):
                    conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', second,
                        json.dumps({'events': [{'kind': 'collector_heartbeat', 'source_exists': True}]})))
                for bucket in range(start, end, 300):
                    is_overflow = bucket == target
                    events = [{'kind': 'log', 'text': '[PACKET_WINDOW] ' + json.dumps({
                        'start': bucket, 'end': bucket + 300,
                        'total': 100 if is_overflow else 0, 'signatures': 3 if is_overflow else 0})}]
                    if is_overflow:
                        events.append({'kind': 'log', 'text': '[PACKET_OVERFLOW] 100'})
                    conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', bucket + 301,
                        json.dumps({'events': events})))
            result = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(result['observation_complete'])
            self.assertEqual(result['candidate_total'], 0)
            self.assertEqual(result['missing_profile_buckets'], 1)
            self.assertEqual(result['incomplete_profile_windows'], 1)
            self.assertEqual(result['overflowed_profile_windows'], 1)
            self.assertEqual(result['baseline_days_configured'], 7)
            self.assertFalse(any('same_slot_baseline_days_observed' in item for item in result['candidates']))

    def test_physical_storage_counts_rotations_and_wal_separately_from_filesystem(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            db = root / 'data.sqlite'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
            (root / 'log').write_bytes(b'x' * 100)
            (root / 'log.1').write_bytes(b'x' * 50)
            (root / 'unrelated').write_bytes(b'x' * 1000)
            result = storage.measure(db, {'log_path': str(root / 'log')}, 100)
            self.assertEqual(result['source_logs']['file_bytes'], 150)
            self.assertEqual(result['source_logs']['file_count'], 2)
            self.assertGreater(result['monitor_archive']['logical_bytes'], 0)
            self.assertIn('available_bytes', result['monitor_filesystem'])
            self.assertNotIn(str(root), json.dumps(result))
            (root / 'log.1').unlink()
            after = storage.measure(db, {'log_path': str(root / 'log')}, 200, result)
            self.assertEqual(after['source_logs']['retained_change_bytes'], -50)
            with patch.object(storage.os, 'statvfs', side_effect=OSError):
                self.assertFalse(storage.disk(root)['available'])

    def test_range_marks_overflow_and_missing_coverage_without_exporting_raw_frames(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                profiles = []
                for i in range(1100):
                    item = {'signature': '99:01:02:03', 'known': False, 'count': 1,
                            'first': 100, 'last': 101, 'samples': ['PRIVATE_RAW']}
                    profiles.append({'kind': 'log', 'text': '[PACKET_PROFILE] ' + json.dumps(item)})
                conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', 150, json.dumps({'events': profiles})))
            result = packet_ranges.read(db, 'home', 90, 160, 200)
            self.assertTrue(result['truncated'])
            self.assertFalse(result['observation_complete'])
            self.assertLessEqual(len(json.dumps(result).encode()), 200000)
            self.assertNotIn('PRIVATE_RAW', json.dumps(result))

    def test_candidate_scan_marks_actual_limit_overflow_and_keeps_recent_rows(self):
        with tempfile.TemporaryDirectory() as name:
            db = Path(name) / 'data.sqlite'
            now = 8 * 86400 + 12 * 3600
            end = int(now // 300) * 300
            start = end - 24 * 3600
            heartbeat = json.dumps({'events': [{'kind': 'collector_heartbeat', 'source_exists': True}]})
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE batches (node, collected, body)')
                conn.executemany('INSERT INTO batches VALUES (?,?,?)', (
                    ('home', start + index * 2, heartbeat)
                    for index in range(packet_ranges.MAX_SCAN_BATCHES)))
            exact = packet_ranges.discover_candidates(db, 'home', now)
            self.assertFalse(exact['truncated'])  # exactly the declared cap is complete with respect to scan size
            self.assertEqual(exact['batches_scanned'], packet_ranges.MAX_SCAN_BATCHES)
            self.assertEqual(exact['older_rows_omitted_at_least'], 0)
            with sqlite3.connect(db) as conn:
                history_start = start - 7 * 86400
                conn.execute('INSERT INTO batches VALUES (?,?,?)', ('home', history_start + 1, heartbeat))
            overflow = packet_ranges.discover_candidates(db, 'home', now)
            self.assertTrue(overflow['truncated'])
            self.assertEqual(overflow['matching_rows_detected_at_least'], packet_ranges.MAX_SCAN_BATCHES + 1)
            self.assertEqual(overflow['older_rows_omitted_at_least'], 1)
            self.assertFalse(overflow['observation_complete'])
            self.assertLess(overflow['collector_max_gap_seconds'], 86400)  # latest current-window rows were retained
