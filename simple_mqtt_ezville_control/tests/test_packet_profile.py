import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packet_profile import PacketProfiler


class PacketProfileTests(unittest.TestCase):
    def test_confirmed_elevator_signatures_and_car_marker(self):
        lines = []
        profiler = PacketProfiler(lines.append)
        profiler.started, profiler.next_flush = 100, 160
        for frame in ('F733014301112233', 'F73301C303112233'):
            profiler.observe(frame, False, now=101)
        self.assertTrue(profiler.mark('car_entry', now=102))
        self.assertFalse(profiler.mark('car_entry', now=103))
        profiler.flush(160)
        profiles = [json.loads(line.split('] ', 1)[1]) for line in lines if line.startswith('[PACKET_PROFILE]')]
        self.assertTrue(all(profile['known'] for profile in profiles))
        self.assertEqual(sum(profile['count'] for profile in profiles), 2)
        self.assertIn('car_entry', next(line for line in lines if line.startswith('[PACKET_MARK]')))

    def test_all_validated_frames_are_counted_with_bounded_samples(self):
        lines = []
        profiler = PacketProfiler(lines.append)
        profiler.started, profiler.next_flush = 100, 160
        for i in range(500):
            profiler.observe('F799010200' + f'{i:04X}', False, now=101 + i / 1000)
        profiler.flush(160)
        profiles = [json.loads(line.split('] ', 1)[1]) for line in lines if line.startswith('[PACKET_PROFILE]')]
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0]['count'], 500)
        self.assertEqual(len(profiles[0]['samples']), 3)
        self.assertEqual(len(profiles[0]['variants']), 8)
        self.assertEqual(profiles[0]['other_variant_count'], 492)
        self.assertEqual(profiles[0]['variant_changes'], 499)
        self.assertNotIn('_previous_variant', profiles[0])

    def test_repeated_variants_keep_exact_counts_without_raw_extra_samples(self):
        lines = []
        profiler = PacketProfiler(lines.append)
        profiler.started, profiler.next_flush = 100, 160
        for packet in ['F799010200AA', 'F799010200AA', 'F799010200BB', 'F799010200AA']:
            profiler.observe(packet, False, now=101)
        profiler.flush(160)
        profile = json.loads(next(line.split('] ', 1)[1] for line in lines
                                  if line.startswith('[PACKET_PROFILE]')))
        self.assertEqual(sorted(profile['variants'].values()), [1, 3])
        self.assertEqual(profile['variant_changes'], 2)
        self.assertEqual(profile['bucket_start'], 100)

    def test_marker_retains_before_after_evidence_without_transmitting(self):
        lines = []
        profiler = PacketProfiler(lines.append)
        profiler.started, profiler.next_flush = 100, 160
        profiler.observe('F799010200AA', False, now=100)
        self.assertTrue(profiler.mark('bell', now=102))
        profiler.observe('F799010200BB', False, now=103)
        self.assertFalse(profiler.mark('door_open', now=104))
        traces = [json.loads(line.split('] ', 1)[1]) for line in lines if line.startswith('[PACKET_TRACE]')]
        self.assertEqual([item['offset_ms'] for item in traces], [-2000, 1000])
        self.assertFalse(profiler.mark('reboot', now=180))


if __name__ == '__main__':
    unittest.main()
