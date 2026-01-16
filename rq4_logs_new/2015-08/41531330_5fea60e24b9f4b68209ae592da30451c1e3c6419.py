import time
import threading


class SerialDispatcher(object):
    THREAD_NAME = 'Method dispatcher'

    def __init__(self, address='localhost', port=5000, sleep=None):
        self.sleep = sleep
        self._initialize_thread()

    def _initialize_thread(self):
        self.thread = threading.Thread(
            target=self._run, name=SerialDispatcher.THREAD_NAME, args=(),
        )
        self.thread.daemon = True

    def _run(self):
        print 'running now'

        time.sleep(self.sleep)

    def start_daemon(self):
        self.thread.start()