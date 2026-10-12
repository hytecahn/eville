import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from packet_profile import PacketProfiler


class PacketProfileTests(unittest.TestCase):
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
