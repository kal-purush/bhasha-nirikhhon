import redis
import time
import random
from threading import Thread
from threading import Timer
r = redis.Redis(host='localhost', port=6379, db=0)

M = 30
T = 10
W = 20
C = 10
N = 2
thread_list = []
id_list = []
heartbeats = "heartbeats"
fib1 = "fib1"
fib2 = "fib"
mylock = "lock"

def calculate_fib():
    lock = r.setnx(mylock, 1)
    while lock == 0:
        lock = r.setnx(mylock, 1)
    nr_fib1 = int(r.get(fib1))
    nr_fib2 = int(r.get(fib2))
    aux = nr_fib2
    nr_fib2 = nr_fib2 + nr_fib1
    nr_fib1 = aux
    r.set(fib2, nr_fib2)
    r.set(fib1, nr_fib1)
    print(nr_fib2)
    r.delete(mylock)

def worker_code(k):
    print("Hello from worker: ", end = "")
    print(k)
    t = 1
    actual_score = time.time()
    for i in id_list:
        aux_score = r.zscore(heartbeats, i)
        if aux_score < actual_score - T:
            print(i + " was removed.")
            r.zrem(heartbeats, i)
            id_list.remove(i)

    if len(id_list) < N:
        worker = 'worker' + str(k)
        score = time.time()
        r.zadd(heartbeats, worker, score)
        id_list.append(worker)
        print(worker + " started running.")
    else:
        print("Worker" + str(k) + " didn't have enough space.")
        return
    own_timerT = time.time()
    own_timerC = time.time()
    own_timerW = time.time()
    while 1 > 0:
        if own_timerT < time.time() - T:
            worker = 'worker' + str(k)
            score = time.time()
            r.zadd(heartbeats, worker, score)
            own_timerT = time.time()
            print("Worker" + str(k) + " updated his time.")
        if own_timerC < time.time() - C:
            random_value = random.randint(1, 100)
            if random_value % 10 == 0:
                print("Worker" + str(k) + " crashed.")
                return
            own_timerC = time.time()
        if own_timerW < time.time() - W:
            calculate_fib()
            own_timerW = time.time()

def manager_code():
    t = 0
    k = 0
    r.set(fib1, 1)
    r.set(fib2, 1)
    while 1 > 0:
        print("Master creates thread number: ", end="")
        t = Thread(target = worker_code, args = [k])
        thread_list.append(t)
        k = k + 1
        t.start()
        time.sleep(M)

if __name__ == "__main__":
    manager_code()