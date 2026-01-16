import signal
import socket
from sys import argv


class TimeoutException(Exception):   # Custom exception class
    pass


def timeout_handler(signum, frame):   # Custom signal handler
    raise TimeoutException


# Change the behavior of SIGALRM
signal.signal(signal.SIGALRM, timeout_handler)

if __name__ == "__main__":
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', 8002))
            signal.alarm(1)
            executed = 0
            while True:
                try:
                    s.send(argv[1].encode('utf-8') + b'\n')
                    s.recv(100)
                    executed += 1
                except TimeoutException:
                    print("last rps: ", executed)
                    signal.alarm(0)
                    break