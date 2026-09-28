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

    def publish(self, topic, payload):
        self.publications.append((topic, payload))
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

    async def test_split_frame_and_state_publication(self):
        client = await self.start()
        raw = bytes.fromhex(STATE)
        client.emit('ew11/recv', raw[:7]); client.emit('ew11/recv', raw[7:])
        await self.wait_for(lambda: any(t == 'ezville/thermostat_01_01/setTemp/state' for t, _ in client.publications))
        self.assertIn(('ezville/thermostat_01_01/setTemp/state', b'18'), client.publications)

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
        client.emit('ezville/batch_01_01/elevator-down/command', b'PRESS')
        await self.wait_for(lambda: any(t == 'ew11/send' for t, _ in client.publications))
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
