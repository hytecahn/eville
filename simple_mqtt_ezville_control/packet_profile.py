"""Bounded, passive profile of checksum-valid wallpad frames; never transmits a packet."""
from collections import Counter, deque
import hashlib
import json
import time

MAX_SIGNATURES = 256
MAX_TRACE = 300
MAX_VARIANTS = 8


def signature(packet):
    return ':'.join((packet[2:4], packet[4:6], packet[6:8], packet[8:10]))


class PacketProfiler:
    def __init__(self, write, interval=300):
        self.write = write
        self.interval = interval
        self.started = time.time()
        self.next_flush = (int(self.started // interval) + 1) * interval
        self.patterns = {}
        self.overflow = 0
        self.history = deque(maxlen=100)  # Leave at least 200 samples for after a marker.
        self.trace = None
        self.last_mark = 0

    def observe(self, packet, known, now=None):
        now = time.time() if now is None else now
        if now >= self.next_flush:
            self.flush(now)
        key = signature(packet)
        entry = self.patterns.get(key)
        if entry is None:
            if len(self.patterns) >= MAX_SIGNATURES:
                self.overflow += 1
            else:
                entry = self.patterns[key] = {'signature': key, 'known': bool(known),
                    'count': 0, 'first': now, 'last': now, 'samples': [],
                    'bucket_start': self.started, 'variants': {}, 'other_variant_count': 0,
                    'variant_changes': 0}
        if entry is not None:
            # A 128-bit digest keeps the retained five-minute profile compact.
            fingerprint = hashlib.sha256(packet.encode('ascii')).hexdigest()[:32]
            if entry.get('_previous_variant') is not None and entry['_previous_variant'] != fingerprint:
                entry['variant_changes'] += 1
            entry['_previous_variant'] = fingerprint
            variants = entry['variants']
            if fingerprint in variants:
                variants[fingerprint] += 1
            elif len(variants) < MAX_VARIANTS:
                variants[fingerprint] = 1
            else:
                entry['other_variant_count'] += 1
            entry['count'] += 1
            entry['last'] = now
            if packet not in entry['samples'] and len(entry['samples']) < 3:
                entry['samples'].append(packet[:256])
        self.history.append((now, key, packet[:256]))
        if self.trace and now <= self.trace['end'] and self.trace['remaining']:
            self._sample(now, key, packet)
        if self.trace and now > self.trace['end']:
            self.trace = None

    def mark(self, label, now=None):
        now = time.time() if now is None else now
        if self.trace and now > self.trace['end']:
            self.trace = None
        if label not in ('bell', 'door_open', 'other') or self.trace or now - self.last_mark < 60:
            return False
        self.last_mark = now
        self.trace = {'label': label, 'at': now, 'end': now + 15, 'remaining': MAX_TRACE}
        self.write('[PACKET_MARK] ' + json.dumps({'label': label, 'at': now}, separators=(',', ':')))
        for at, signature, packet in list(self.history):
            if now - 5 <= at <= now and self.trace['remaining']:
                self._sample(at, signature, packet)
        return True

    def _sample(self, at, signature, packet):
        self.trace['remaining'] -= 1
        self.write('[PACKET_TRACE] ' + json.dumps({'label': self.trace['label'], 'mark_at': self.trace['at'],
            'offset_ms': round((at - self.trace['at']) * 1000), 'signature': signature,
            'sample': packet[:256]}, separators=(',', ':')))

    def flush(self, now=None):
        now = time.time() if now is None else now
        self.write('[PACKET_WINDOW] ' + json.dumps({'start': self.started, 'end': now,
            'total': sum(entry['count'] for entry in self.patterns.values()) + self.overflow,
            'signatures': len(self.patterns)}, separators=(',', ':')))
        for entry in self.patterns.values():
            self.write('[PACKET_PROFILE] ' + json.dumps({key: value for key, value in entry.items()
                if not key.startswith('_')}, separators=(',', ':')))
        if self.overflow:
            self.write('[PACKET_OVERFLOW] ' + str(self.overflow))
        self.patterns = {}
        self.overflow = 0
        self.started = now
        self.next_flush = (int(now // self.interval) + 1) * self.interval

    def tick(self, now=None):
        now = time.time() if now is None else now
        if now >= self.next_flush:
            self.flush(now)
