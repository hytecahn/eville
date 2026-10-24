import asyncio
import json
import os
from pathlib import Path
import signal
import socket
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ezville
from runtime import bounded_call
from watchdog import ProcessSupervisor

ROOT = Path(__file__).resolve().parents[1]
STATE = 'F7361F810D800B040000121B121A051A131CCCDC'


class FakeClient:
    instances = []
    reject_first = False

    def __init__(self, *args, **kwargs):
        self.publications = []
        self.publication_retains = []
        self.stopped = False
        self.disconnected = False
        self.on_connect = self.on_disconnect = self.on_message = None
        self.__class__.instances.append(self)

    def username_pw_set(self, *args): pass
    def reconnect_delay_set(self, **kwargs): pass
    def connect_async(self, *args, **kwargs): pass
    def subscribe(self, *args): return (0, 1)

    def loop_start(self):
        self.on_connect(self, None, {}, 0, None)

    def loop_stop(self): self.stopped = True
    def disconnect(self): self.disconnected = True

    def publish(self, topic, payload, retain=False):
        self.publications.append((topic, payload))
        self.publication_retains.append((topic, retain))
        if self.reject_first and len(self.__class__.instances) == 1:
            return SimpleNamespace(rc=4)
        return SimpleNamespace(rc=0)

    def emit(self, topic, payload, retain=False):
        self.on_message(self, None, SimpleNamespace(topic=topic, payload=payload, retain=retain))


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        FakeClient.instances = []
        FakeClient.reject_first = False
        self.logs = []
        self.patches = [patch.object(ezville.mqtt, 'Client', FakeClient), patch.object(ezville, 'log', self.logs.append)]
        for item in self.patches: item.start()
        self.config = json.loads((ROOT / 'config.json').read_text())['options']
        self.config.update(discovery_delay=0, state_loop_delay=.001, command_loop_delay=.001,
                           restart_check_delay=.01, first_waittime=.001, command_interval=.001,
                           command_retry_count=1, DEBUG_LOG=True)
        self.worker = None

    async def asyncTearDown(self):
        if self.worker:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
        for item in self.patches: item.stop()

    async def wait_for(self, predicate, seconds=3):
        deadline = asyncio.get_running_loop().time() + seconds
        while not predicate():
            if self.worker and self.worker.done():
                self.worker.result()
            if asyncio.get_running_loop().time() >= deadline:
                self.fail('condition timed out; logs=' + repr(self.logs[-8:]))
            await asyncio.sleep(.005)

    async def start(self):
        self.worker = asyncio.create_task(ezville.ezville_loop(self.config))
        await self.wait_for(lambda: FakeClient.instances)
        await asyncio.sleep(.01)
        return FakeClient.instances[-1]

    async def test_outage_alert_and_recovery_backoff_survive_session_restart(self):
        self.config['ew11_timeout'] = .05
        with patch.object(ezville.telnetlib, 'Telnet', side_effect=OSError('unreachable')) as reset:
            await self.start()
            await self.wait_for(lambda: len(FakeClient.instances) >= 2)
            client = FakeClient.instances[-1]
            await asyncio.sleep(.15)
            statuses = [json.loads(p)['status'] for c in FakeClient.instances
                        for t, p in c.publications if t == 'ezville/ew11/status']
            self.assertEqual(statuses, ['offline'])
            self.assertEqual(reset.call_count, 1)
            async def healthy_bus():
                while True:
                    client.emit('ew11/recv', bytes.fromhex(STATE))
                    await asyncio.sleep(.005)
            packets = asyncio.create_task(healthy_bus())
            try:
                await self.wait_for(lambda: any(t == 'ezville/ew11/status' and
                                    json.loads(p)['status'] == 'online'
                                    for t, p in client.publications))
            finally:
                packets.cancel()
                await asyncio.gather(packets, return_exceptions=True)
            await self.wait_for(lambda: reset.call_count == 2)
            statuses = [json.loads(p)['status'] for c in FakeClient.instances
                        for t, p in c.publications if t == 'ezville/ew11/status']
            self.assertEqual(statuses, ['offline', 'online', 'offline'])

    async def exercise_rejected_status(self, rejected_status, recover_before_accept=False):
        self.config['ew11_timeout'] = .05
        accepted, rejected = [], []
        reject = True
        original = FakeClient.publish

        def publish(client, topic, payload, retain=False):
            if topic == 'ezville/ew11/status':
                status = json.loads(payload)['status']
                if reject and status == rejected_status:
                    rejected.append(status)
                    return SimpleNamespace(rc=4)
                accepted.append(status)
            return original(client, topic, payload, retain)

        async def healthy_bus():
            while True:
                FakeClient.instances[-1].emit('ew11/recv', bytes.fromhex(STATE))
                await asyncio.sleep(.005)

        packets = None
        with patch.object(FakeClient, 'publish', publish), patch.object(
                ezville.telnetlib, 'Telnet', side_effect=OSError('unreachable')) as reset:
            try:
                await self.start()
                await self.wait_for(lambda: len(FakeClient.instances) >= 2)
                if rejected_status == 'online':
                    self.assertEqual(accepted, ['offline'])
                    packets = asyncio.create_task(healthy_bus())
                await self.wait_for(lambda: len(rejected) >= 2)
                self.assertEqual(reset.call_count, 1)
                if recover_before_accept:
                    packets = asyncio.create_task(healthy_bus())
                    await self.wait_for(lambda: accepted == ['online'])
                reject = False
                expected = ['offline', 'online'] if rejected_status == 'online' else (
                    ['online'] if recover_before_accept else ['offline'])
                await self.wait_for(lambda: accepted == expected)
                await asyncio.sleep(.15)
                self.assertEqual(accepted, expected)
                self.assertEqual(reset.call_count, 1)
                if rejected_status == 'online':
                    packets.cancel()
                    await asyncio.gather(packets, return_exceptions=True)
                    packets = None
                    await self.wait_for(lambda: accepted == ['offline', 'online', 'offline'])
            finally:
                if packets:
                    packets.cancel()
                    await asyncio.gather(packets, return_exceptions=True)

    async def test_rejected_offline_retries_across_session_restart(self):
        await self.exercise_rejected_status('offline')

    async def test_rejected_online_retries_without_more_physical_recovery(self):
        await self.exercise_rejected_status('online')

    async def test_recovery_replaces_rejected_offline_with_latest_online(self):
        await self.exercise_rejected_status('offline', recover_before_accept=True)

    async def test_split_frame_and_state_publication(self):
        client = await self.start()
        raw = bytes.fromhex(STATE)
        client.emit('ew11/recv', raw[:7]); client.emit('ew11/recv', raw[7:])
        await self.wait_for(lambda: any(t == 'ezville/thermostat_01_01/setTemp/state' for t, _ in client.publications))
        self.assertIn(('ezville/thermostat_01_01/setTemp/state', b'18'), client.publications)

    async def test_discovery_is_retained_and_ha_birth_recovers_without_reboot_control(self):
        self.config['reboot_control'] = False
        client = await self.start()
        state_topic = 'ezville/thermostat_01_01/setTemp/state'
        config_topic = 'homeassistant/climate/ezville_wallpad/ezville_thermostat_01_01/config'
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: any(t == state_topic for t, _ in client.publications))
        self.assertIn((config_topic, True), client.publication_retains)
        self.assertIn((state_topic, False), client.publication_retains)
        original_config = next(p for t, p in client.publications if t == config_topic)
        await asyncio.sleep(.02)
        client.publications.clear()
        # HA reloading does not reconnect the still healthy add-on MQTT client.
        client.emit('homeassistant/status', b'online')
        await asyncio.sleep(.01)
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: any(t == state_topic for t, _ in client.publications))
        self.assertEqual(next(p for t, p in client.publications if t == config_topic), original_config)
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertFalse(any(t == 'ew11/send' for t, _ in client.publications))
        self.assertTrue(all(not retained for topic, retained in client.publication_retains if not topic.startswith('homeassistant/')))

    async def test_corrupt_fragment_does_not_poison_next_frame(self):
        client = await self.start()
        client.emit('ew11/recv', bytes.fromhex('F7361F810D'))
        client.emit('ew11/recv', bytes(20))
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: any(t.endswith('/setTemp/state') for t, _ in client.publications))

    async def test_off_thermostat_without_away_bit(self):
        client = await self.start()
        raw = STATE[:12] + '00' + '00' + STATE[16:]
        raw = ezville.checksum(raw)
        client.emit('ew11/recv', bytes.fromhex(raw))
        await self.wait_for(lambda: any(t.endswith('/power/state') for t, _ in client.publications))
        self.assertIn(('ezville/thermostat_01_01/power/state', b'off'), client.publications)

    async def test_invalid_and_retained_commands_do_not_kill_task(self):
        client = await self.start()
        client.emit('ezville/thermostat_01_01/setTemp/command', b'nan')
        client.emit('ezville/thermostat_01_01/power/command', b'invalid')
        client.emit('ezville/batch_01_01/elevator-down/command', b'PRESS', retain=True)
        await asyncio.sleep(.03)
        self.assertFalse(any(t == 'ew11/send' for t, _ in client.publications))
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: any(t.endswith('/setTemp/state') for t, _ in client.publications))
        client.emit('ezville/batch_01_01/elevator-down/command', b'PRESS')
        await self.wait_for(lambda: any(t == 'ew11/send' for t, _ in client.publications))
        await self.wait_for(lambda: any('[EVENT] elevator_call_sent direction=down' in line for line in self.logs))
        self.assertEqual(sum('[EVENT] elevator_call_sent' in line for line in self.logs), 1)
        self.assertEqual(len(FakeClient.instances), 1)

    async def test_checksum_valid_but_short_state_frame_is_rejected(self):
        client = await self.start()
        short = ezville.checksum('F7361F8101000000')
        client.emit('ew11/recv', bytes.fromhex(short + STATE))
        await self.wait_for(lambda: any(t.endswith('/setTemp/state') for t, _ in client.publications))
        self.assertEqual(len(FakeClient.instances), 1)

    async def test_elevator_arrival_protocol_still_publishes(self):
        client = await self.start()
        client.emit('ew11/recv', bytes.fromhex('F7330143018007F6'))
        await self.wait_for(lambda: any(t == 'ezville/elevator/arrival' for t, _ in client.publications))
        event = json.loads(next(p for t, p in client.publications if t == 'ezville/elevator/arrival'))
        self.assertEqual(event['event'], 'elevator_arrival')
        self.assertGreater(event['timestamp'], 1700000000)
        self.assertTrue(any('[EVENT] elevator_arrival_signal' in line for line in self.logs))

    async def test_elevator_rejected_without_fresh_bus_and_not_replayed(self):
        client = await self.start()
        client.emit('ezville/batch_01_01/elevator-down/command', b'PRESS')
        await self.wait_for(lambda: any(t == 'ezville/elevator/result' for t, _ in client.publications))
        self.assertFalse(any(t == 'ew11/send' for t, _ in client.publications))
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await asyncio.sleep(.05)
        self.assertFalse(any(t == 'ew11/send' for t, _ in client.publications))

    async def test_actual_local_elevator_variants_are_evidence_not_arrival(self):
        client = await self.start()
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: any(t.endswith('/setTemp/state') for t, _ in client.publications))
        client.emit('ezville/batch_01_01/elevator-down/command', b'PRESS')
        await self.wait_for(lambda: any(t == 'ew11/send' for t, _ in client.publications))
        # Actual production samples, including a split frame and duplicate polls.
        client.emit('ew11/recv', bytes.fromhex('F733014301'))
        client.emit('ew11/recv', bytes.fromhex('109716F73301C30300040001F6'))
        await self.wait_for(lambda: sum(t == 'ezville/elevator/evidence' for t, _ in client.publications) == 2)
        evidence = [json.loads(p) for t, p in client.publications if t == 'ezville/elevator/evidence']
        self.assertEqual([e['kind'] for e in evidence], ['signal_candidate', 'response'])
        self.assertTrue(all(e.get('request_id') and e['elapsed_seconds'] >= 0 for e in evidence))
        self.assertTrue(all(not e['arrival_verified'] for e in evidence))
        self.assertFalse(any(t == 'ezville/elevator/arrival' for t, _ in client.publications))

    async def test_corrupt_traffic_does_not_prevent_outage_detection(self):
        self.config['ew11_timeout'] = .08
        with patch.object(ezville.telnetlib, 'Telnet', side_effect=OSError('unreachable')) as reset:
            await self.start()
            for _ in range(30):
                FakeClient.instances[-1].emit('ew11/recv', bytes(10))
                await asyncio.sleep(.01)
            self.assertGreaterEqual(reset.call_count, 1)
            statuses = [json.loads(p)['status'] for c in FakeClient.instances
                        for t, p in c.publications if t == 'ezville/ew11/status']
            self.assertEqual(statuses, ['offline'])

    async def test_physical_recovery_survives_session_restart(self):
        self.config['ew11_timeout'] = .05
        entered, release = asyncio.Event(), asyncio.Event()
        original = ezville.bounded_call
        async def delayed(function, timeout):
            if timeout == 30:
                entered.set()
                await release.wait()
                raise OSError('test reset completion')
            return await original(function, timeout)
        with patch.object(ezville, 'bounded_call', delayed):
            await self.start()
            await self.wait_for(entered.is_set)
            await self.wait_for(lambda: len(FakeClient.instances) >= 2)
            # Old session has ended but the reset coroutine is still alive.
            release.set()
            await self.wait_for(lambda: any('test reset completion' in l for l in self.logs))
            self.assertTrue(any('유효 패킷 없음' in l for l in self.logs))

    async def test_cleanup_timeout_exits_worker_instead_of_reusing_client(self):
        client = await self.start()
        async def failed_cleanup(function, timeout):
            raise asyncio.TimeoutError('network thread stuck')
        with patch.object(ezville, 'bounded_call', failed_cleanup):
            os.kill(os.getpid(), signal.SIGTERM)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(self.worker, 1)
        self.assertEqual(len(FakeClient.instances), 1)

    async def test_debug_command_before_state_does_not_raise_typeerror(self):
        client = await self.start()
        client.emit('ezville/light_01_01/power/command', b'ON')
        await self.wait_for(lambda: any('command failed' in line for line in self.logs))
        self.assertFalse(any('TypeError' in line for line in self.logs))
        self.assertEqual(len(FakeClient.instances), 1)

    async def test_reconnect_republishes_unchanged_state(self):
        client = await self.start()
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: any(t.endswith('/setTemp/state') for t, _ in client.publications))
        await asyncio.sleep(.03)
        client.publications.clear()
        client.on_connect(client, None, {}, 0, None)
        await asyncio.sleep(.01)
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: any(t.endswith('/setTemp/state') for t, _ in client.publications))

    async def test_publish_failure_recreates_client_and_cleans_old_tasks(self):
        FakeClient.reject_first = True
        client = await self.start()
        client.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: len(FakeClient.instances) == 2)
        self.assertTrue(client.stopped and client.disconnected)
        fresh = FakeClient.instances[-1]
        await asyncio.sleep(.01)
        fresh.emit('ew11/recv', bytes.fromhex(STATE))
        await self.wait_for(lambda: any(t.endswith('/setTemp/state') for t, _ in fresh.publications))

    async def test_receive_overflow_recovers_instead_of_growing_forever(self):
        client = await self.start()
        for _ in range(1100): client.emit('ew11/recv', b'\0')
        await self.wait_for(lambda: len(FakeClient.instances) == 2)
        self.assertTrue(any('queue overflow' in line for line in self.logs))
        self.assertTrue(client.stopped)

    async def test_socket_silence_does_not_block_loop_and_eof_recovers(self):
        writers = []
        async def accept(reader, writer):
            writers.append(writer)
        server = await asyncio.start_server(accept, '127.0.0.1', 0)
        self.config.update(mode='socket', ew11_server='127.0.0.1', ew11_port=server.sockets[0].getsockname()[1])
        try:
            client = await self.start()
            await self.wait_for(lambda: writers)
            await asyncio.sleep(.05)  # would hang forever with blocking recv()
            self.assertFalse(self.worker.done())
            writers[0].close()
            await writers[0].wait_closed()
            await self.wait_for(lambda: len(FakeClient.instances) == 2)
            self.assertTrue(client.stopped)
        finally:
            server.close()
            for writer in writers: writer.close()
            await server.wait_closed()

    async def test_socket_chunks_have_distinct_payload_objects(self):
        writers = []
        async def accept(reader, writer): writers.append(writer)
        server = await asyncio.start_server(accept, '127.0.0.1', 0)
        self.config.update(mode='socket', ew11_server='127.0.0.1', ew11_port=server.sockets[0].getsockname()[1],
                           state_loop_delay=.1, serial_recv_delay=.001)
        try:
            client = await self.start()
            await self.wait_for(lambda: writers)
            raw = bytes.fromhex(STATE)
            writers[0].write(raw[:9]); await writers[0].drain()
            await asyncio.sleep(.02)
            writers[0].write(raw[9:]); await writers[0].drain()
            await self.wait_for(lambda: any(t.endswith('/setTemp/state') for t, _ in client.publications))
        finally:
            server.close()
            for writer in writers: writer.close()
            await server.wait_closed()

    async def test_sigterm_cleans_worker_without_restart(self):
        client = await self.start()
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(self.worker, 1)
        self.assertTrue(client.stopped and client.disconnected)
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertFalse(any('ezville_loop.<locals>' in str(t.get_coro()) for t in asyncio.all_tasks() if t is not asyncio.current_task()))

    async def test_blocking_io_timeout_leaves_event_loop_responsive(self):
        with self.assertRaises(asyncio.TimeoutError):
            await bounded_call(lambda: time.sleep(.1), .02)
        await asyncio.sleep(.01)


class WatchdogTests(unittest.TestCase):
    def supervisor(self, code, **kwargs):
        self.messages = []
        return ProcessSupervisor([sys.executable, '-u', '-c', code], timeout=.25, grace=.2, poll=.02,
                                 report=self.messages.append, **kwargs)

    def test_stalled_process_is_terminated_independently(self):
        code = "import os,time,signal; signal.signal(signal.SIGUSR1,signal.SIG_IGN); os.write(int(os.environ['EZVILLE_HEARTBEAT_FD']),b'.'); time.sleep(30)"
        supervisor = self.supervisor(code)
        started = time.monotonic()
        self.assertNotEqual(supervisor.run_once(), 0)
        self.assertLess(time.monotonic() - started, 2)
        self.assertTrue(any('stalled' in message for message in self.messages))

    def test_healthy_heartbeat_is_not_killed(self):
        code = "import os,time; fd=int(os.environ['EZVILLE_HEARTBEAT_FD']);\nfor _ in range(10):\n os.write(fd,b'.'); time.sleep(.04)\n"
        supervisor = self.supervisor(code)
        supervisor.run_once()
        self.assertFalse(any('stalled' in message for message in self.messages))

    def test_crashed_process_is_restarted_with_backoff(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'starts'
            code = "from pathlib import Path; import os; p=Path({!r}); p.write_text(p.read_text()+'x' if p.exists() else 'x'); os._exit(7)".format(str(path))
            supervisor = self.supervisor(code)
            timer = threading.Timer(1.5, supervisor.stop)
            timer.start()
            try: supervisor.run()
            finally: timer.cancel(); supervisor.terminate()
            self.assertEqual(path.read_text(), 'xx')

    def test_shutdown_does_not_respawn_child(self):
        code = "import os,time,signal; signal.signal(signal.SIGTERM,lambda *_:exit(0)); fd=int(os.environ['EZVILLE_HEARTBEAT_FD']);\nwhile True:\n os.write(fd,b'.'); time.sleep(.03)"
        supervisor = self.supervisor(code)
        timer = threading.Timer(.15, supervisor.stop)
        timer.start()
        try: supervisor.run()
        finally: timer.cancel(); supervisor.terminate()
        self.assertEqual(supervisor.process.returncode, 0)
        self.assertFalse(any('restart in' in message for message in self.messages))


if __name__ == '__main__': unittest.main()

