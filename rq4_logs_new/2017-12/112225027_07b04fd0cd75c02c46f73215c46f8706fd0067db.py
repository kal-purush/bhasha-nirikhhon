import threading
import time

class my_thread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    ex=True

    def __init__(self):
        super(my_thread, self).__init__()
        self._stop_event = threading.Event()
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()  

    def stop(self):
        ex=False
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()
    def run(self):
        """ Method that runs forever """
        while not self.stopped():
            # Do something
            print('Doing something imporant in the background')
            time.sleep(5)


t = my_thread()
time.sleep(25)
t.stop()