"""Exercise real Paho callbacks over TCP, without a production MQTT broker."""
import asyncio
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ezville

STATE = bytes.fromhex('F7361F810D800B040000121B121A051A131CCCDC')


def frame(header, body):
    size = len(body)
    encoded = bytearray([header])
    while True:
        digit = size % 128
        size //= 128
        encoded.append(digit | (128 if size else 0))
        if not size: break
    return bytes(encoded) + body


class MQTTWireTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_paho_reconnects_resubscribes_and_republishes(self):
        publications, writers, handlers, logs = [], [], set(), []
        connections = 0

        async def broker(reader, writer):
            nonlocal connections
            connections += 1
            writers.append(writer)
            task = asyncio.current_task()
            handlers.add(task)
            try:
                while True:
                    header = (await reader.readexactly(1))[0]
                    length, multiplier = 0, 1
                    while True:
                        digit = (await reader.readexactly(1))[0]
                        length += (digit & 127) * multiplier
                        if not digit & 128: break
                        multiplier *= 128
                    body = await reader.readexactly(length)
                    kind = header >> 4
                    if kind == 1:
                        writer.write(b'\x20\x02\x00\x00')
                    elif kind == 8:
                        writer.write(frame(0x90, body[:2] + b'\x00\x00\x01\x00'))
                        topic = b'ew11/recv'
                        writer.write(frame(0x30, len(topic).to_bytes(2, 'big') + topic + STATE))
                    elif kind == 3:
                        size = int.from_bytes(body[:2], 'big')
                        publications.append((body[2:2+size].decode(), body[2+size:]))
                    elif kind == 12:
                        writer.write(b'\xd0\x00')
                    elif kind == 14:
                        break
                    await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()
                handlers.discard(task)

        server = await asyncio.start_server(broker, '127.0.0.1', 0)
        config = json.loads((Path(__file__).resolve().parents[1] / 'config.json').read_text())['options']
        config.update(mqtt_server='127.0.0.1', mqtt_port=server.sockets[0].getsockname()[1],
                      discovery_delay=0, state_loop_delay=.005, command_loop_delay=.005)
        topic = 'ezville/thermostat_01_01/setTemp/state'
        async def wait_for(predicate):
            end = asyncio.get_running_loop().time() + 5
            while not predicate():
                if worker.done(): worker.result()
                if asyncio.get_running_loop().time() > end: self.fail(repr(logs[-5:]))
                await asyncio.sleep(.01)
        with patch.object(ezville, 'log', logs.append):
            worker = asyncio.create_task(ezville.ezville_loop(config))
            try:
                await wait_for(lambda: (topic, b'18') in publications)
                await asyncio.sleep(.05)
                publications.clear()
                writers[0].close()
                await wait_for(lambda: connections >= 2 and (topic, b'18') in publications)
                self.assertTrue(any('MQTT 연결 해제' in line for line in logs))
            finally:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
                server.close()
                for writer in writers: writer.close()
                await asyncio.gather(*list(handlers), return_exceptions=True)
                await server.wait_closed()
