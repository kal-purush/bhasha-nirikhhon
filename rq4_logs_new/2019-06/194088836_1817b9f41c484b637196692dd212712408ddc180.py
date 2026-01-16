from prometheus_client import start_http_server, Counter
import random
import time

if __name__ == '__main__':
    start_http_server(8000)
    c = Counter('sessions_finished', 'number of finished sessions')
    while True:
    	c.inc(2)
    	time.sleep(2)


        