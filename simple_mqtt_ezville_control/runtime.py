"""Small, independently testable lifecycle primitives (Python 3.8+)."""
import asyncio
import concurrent.futures
import os
import threading


async def bounded_call(function, timeout):
    """Do not block asyncio or leave its default executor stuck on shutdown.

    The underlying operation must also have finite I/O timeouts. A timed-out
    cleanup is fatal to the worker; the process supervisor starts a clean one.
    """
    result = concurrent.futures.Future()

    def run():
        if not result.set_running_or_notify_cancel():
            return
        try:
            result.set_result(function())
        except BaseException as error:
            result.set_exception(error)

    threading.Thread(target=run, daemon=True, name='bounded-io').start()
    return await asyncio.wait_for(asyncio.wrap_future(result), timeout)


async def heartbeat_loop(interval=5):
    """Only the event loop writes this pipe: another thread cannot fake health."""
    value = os.environ.get('EZVILLE_HEARTBEAT_FD')
    if value is None:
        while True:
            await asyncio.sleep(interval)
    fd = int(value)
    os.set_blocking(fd, False)
    try:
        while True:
            try:
                os.write(fd, b'.')
            except BlockingIOError:
                pass  # Parent is alive but briefly busy; never block the loop.
            await asyncio.sleep(interval)
    finally:
        os.close(fd)


async def cancel_tasks(tasks):
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
