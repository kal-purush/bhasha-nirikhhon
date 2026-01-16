import time
from threading import Thread, Lock

tasks = []
tasks_lock = Lock()


def add_task(func, interval):
    tasks_lock.acquire()
    tasks.append({
        "func": func,
        "next": time.time(),
        "interval": interval
    })
    tasks_lock.release()


def check_tasks():
    tasks_lock.acquire()
    now = time.time()
    for task in tasks:
        if task["next"] < now:
            task["func"]()
            task["next"] = now+task["interval"]
    tasks_lock.release()


def time_ticker():
    while True:
        check_tasks()
        time.sleep(1)


def init():
    t = Thread(target=time_ticker)
    t.start()


__all__ = ["init", "add_task"]