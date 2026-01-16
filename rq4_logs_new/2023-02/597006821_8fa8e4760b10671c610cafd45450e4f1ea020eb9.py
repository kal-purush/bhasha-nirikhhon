import unittest
import CI_server
import requests
from threading import Thread, Event

class StoppableThread(Thread):
    """
    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition.
    """
    def __init__(self,  *args, **kwargs):
        super(StoppableThread, self).__init__(*args, **kwargs)
        self._stop_event = Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

class CIServerTest(unittest.TestCase):
    def test_get_response(self):
        """
        Test case to see if GET-request response is correct
        Expected response is Default
        """
        r = requests.get('http://127.0.0.1:1337/')
        self.assertEqual(r.text, "Default")
        
if __name__ == '__main__':
    # Start server on its own thread
    server_thread = StoppableThread(target=CI_server.main)
    server_thread.start()
    
    unittest.main()

    # Stop/Close the thread
    server_thread.stop()
    server_thread.join()
