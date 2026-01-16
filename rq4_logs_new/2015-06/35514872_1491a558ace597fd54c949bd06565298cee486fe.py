import socket
import threading
from queue import Queue
import time

target = '127.0.0.1'
print_lock = threading.Lock()
def scan(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        conn = s.connect((target,port))

        with print_lock:
            print('Port %s: OPEN' % port)

        conn.close()
    except:
        pass

def threader():
    while True:
        worker = q.get()
        scan(worker)
        q.task_done()

#Job assignment
q = Queue()

for x in range(30):
    #Creating a background threader
    t = threading.Thread(target=threader)
    t.daemon = True

    t.start()

start = time.time()

for worker in range(1, 65535):
    q.put(worker)

q.join()

print('Entire job took:', time.time()-start)