"""Process supervisor: survives a frozen asyncio loop or MQTT thread."""
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import select
import signal
import subprocess
import sys
import threading
import time


class ProcessSupervisor:
    def __init__(self, command, timeout=120, grace=15, poll=0.5, report=print):
        self.command = command
        self.timeout = timeout
        self.grace = grace
        self.poll = poll
        self.report = report
        self.stopping = threading.Event()
        self.process = None

    def stop(self, *_):
        self.stopping.set()

    def terminate(self):
        process = self.process
        if process is None or process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=self.grace)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        except ProcessLookupError:
            process.wait()

    def run_once(self):
        read_fd, write_fd = os.pipe()
        try:
            env = dict(os.environ, EZVILLE_HEARTBEAT_FD=str(write_fd))
            self.process = subprocess.Popen(
                self.command, env=env, pass_fds=(write_fd,), start_new_session=True
            )
        except BaseException:
            os.close(read_fd)
            raise
        finally:
            os.close(write_fd)
        last_beat = time.monotonic()
        armed = False
        try:
            while not self.stopping.is_set() and self.process.poll() is None:
                ready, _, _ = select.select([read_fd], [], [], self.poll)
                if ready:
                    data = os.read(read_fd, 4096)
                    if not data:
                        self.report('[WATCHDOG] heartbeat pipe closed')
                        break
                    last_beat = time.monotonic()
                    armed = True
                if time.monotonic() - last_beat > self.timeout:
                    self.report('[WATCHDOG] event loop stalled for {}s; restarting worker'.format(self.timeout))
                    if armed:
                        # Worker registers faulthandler before its first heartbeat.
                        try:
                            os.kill(self.process.pid, signal.SIGUSR1)
                        except ProcessLookupError:
                            pass
                        self.stopping.wait(0.2)
                    break
        finally:
            self.terminate()
            os.close(read_fd)
        return self.process.wait()

    def run(self):
        delay = 1
        while not self.stopping.is_set():
            started = time.monotonic()
            code = self.run_once()
            if self.stopping.is_set():
                break
            if time.monotonic() - started >= 300:
                delay = 1
            self.report('[WATCHDOG] worker exited {}; restart in {}s'.format(code, delay))
            self.stopping.wait(delay)
            delay = min(delay * 2, 60)


def main():
    with open('/data/options.json') as stream:
        config = json.load(stream)
    path = config.get('diagnostic_log_file', '/share/simple_mqtt_ezville_control.log') + '.watchdog.log'
    logger = logging.getLogger('watchdog')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(message)s')
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        handler = RotatingFileHandler(path, maxBytes=1024 * 1024, backupCount=3, encoding='utf-8')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except OSError as error:
        print('[WATCHDOG] persistent log unavailable: {}'.format(error), flush=True)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    supervisor = ProcessSupervisor(
        [sys.executable, '-u', '/ezville.py'],
        timeout=max(30, min(600, int(config.get('watchdog_timeout', 120)))),
        report=logger.warning,
    )
    signal.signal(signal.SIGTERM, supervisor.stop)
    signal.signal(signal.SIGINT, supervisor.stop)
    try:
        supervisor.run()
    finally:
        supervisor.terminate()


if __name__ == '__main__':
    main()
