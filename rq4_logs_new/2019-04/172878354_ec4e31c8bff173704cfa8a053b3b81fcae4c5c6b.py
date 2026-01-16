import multiprocessing
import signal
import os
import sys
import select
import logging
import time
from importlib import reload

watchdog_logger = logging.getLogger('watchdog')


def run_monitored(f, heartbeat_timeout=5*60, kill_signal=signal.SIGTERM, select_timeout=15, always_restart=True):
    def monitored(*args, **kwargs):
        read_fd, write_fd = os.pipe()

        stop_now = False
        exit_code = 0

        def heartbeat_func():
            os.write(write_fd, b'\x00')

        def worker_target():
            # Massive booger to blow out all existing logging handlers before forking.
            logging.shutdown()
            reload(logging)
            return f(*args, **kwargs)

        kwargs.update(heartbeat_func=heartbeat_func)
        watchdog_logger.info("Starting watched process...")
        worker_process = multiprocessing.Process(target=worker_target)
        worker_process.start()
        watchdog_logger.info("Watched process started, PID: {0}".format(worker_process.pid))

        last_seen_time = time.time()

        try:
            while not stop_now:
                current_time = time.time()
                read_fds, _, _ = select.select([read_fd], [], [], select_timeout)
                if read_fd in read_fds:
                    watchdog_logger.info("Heartbeat successfully read back from worker.")
                    os.read(read_fd, 1)
                    last_seen_time = time.time()
                if not worker_process.is_alive() and always_restart:
                    watchdog_logger.warning("Working process needs to be restarted, restarting...")
                    worker_process = multiprocessing.Process(target=worker_target)
                    worker_process.start()
                    watchdog_logger.info("New worker process started: PID {0}".format(worker_process.pid))
                    last_seen_time = time.time()
                elif not worker_process.is_alive():
                    stop_now = True
                    exit_code = worker_process.exitcode
                elif current_time - last_seen_time > heartbeat_timeout:
                    watchdog_logger.warning(
                        "No worker process heartbeat seen in {0} seconds.".format(heartbeat_timeout)
                    )
                    watchdog_logger.warning("Killing worker process and restarting...")
                    attempts_left = 10
                    while attempts_left > 0 and worker_process.is_alive():
                        os.kill(worker_process.pid, kill_signal)
                        time.sleep(1)
                    if worker_process.is_alive():
                        watchdog_logger.error(
                            "Unable to kill worker with signal {0}, attempting SIGKILL".format(kill_signal)
                        )
                        os.kill(worker_process.pid, signal.SIGKILL)
                        time.sleep(1)
                    worker_process = multiprocessing.Process(target=worker_target)
                    worker_process.start()
                    watchdog_logger.info("New worker process started: PID {0}".format(worker_process.pid))
                    last_seen_time = time.time()
        except KeyboardInterrupt:
            watchdog_logger.info("SIGINT seen, shutting down...")
        except BaseException as e:
            watchdog_logger.exception("Unhandled exception in watchdog process!")
            raise e
        else:
            sys.exit(exit_code)
        finally:
            if worker_process.is_alive():
                os.kill(worker_process.pid, kill_signal)
    return monitored


if __name__ == "__main__":
    root_logger = logging.getLogger('')
    root_logger.addHandler(logging.StreamHandler())
    root_logger.setLevel(logging.DEBUG)

    def target(heartbeat_func=lambda: None):
        for _ in range(5):
            time.sleep(3)
            heartbeat_func()
    run_monitored(target, heartbeat_timeout=15, select_timeout=5, always_restart=False)()