#coding: utf-8

import signal
import multiprocessing

def _init_pool():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    return

def _make_args(runnable, args):
    return zip((runnable, )*len(args), args)

def _make_worker(args):
    runnable, wargs = args
    w = Worker(runnable, wargs, multi=False)
    return w

class Worker(object):
    def __init__(self, runnable=None, args=(), multi=True):
        self.runnable = runnable
        self.args = args
        self.multi = multi

        return

    def _run(self, channel):
        channel.put('completed')

        if self.multi:
            self.runnable(*self.args)
        else:
            self.runnable(self.args)

        return

class MultiprocessPool(object):
    def __init__(self, nProcess=None):
        self.nProcess = multiprocessing.cpu_count()*2 if nProcess is None else nProcess
        self.running = False

        return

    def __enter__(self):
        self.pool = multiprocessing.Pool(self.nProcess, _init_pool)
        self.channel = multiprocessing.Queue()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.close()
        self.pool.join()
        return

    def is_running(self):
        return self.running

    def _run(self):
        self.running = True
        completed = 0

        while True:
            msg = self.channel.get()
            if msg == 'completed':
                completed += 1
            if completed >= self.nProcess:
                break

        return

    def map(self, runnable, args):
        if self.is_running():return
        assert len(args) > 0

        workers = map(_make_worker, _make_args(runnable, args))
        self.nProcess = len(workers)

        for worker in workers:
            self.pool.apply_async(worker._run(self.channel))

        self._run()

        return

    def run(self, runnable, args=()):
        if self.is_running():return

        for i in range(self.nProcess):
            self.pool.apply_async(Worker(runnable, args)._run(self.channel))

        self._run()

        return


if __name__ == '__main__':
    import time
    def test_run():
        print 'testing run function .....'
        time.sleep(1)

        return

    with MultiprocessPool(2) as mp:
        mp.run(test_run)

    def test_map(x):
        print 'testing map function .....', x
        time.sleep(1)

        return

    with MultiprocessPool() as mp:
        mp.map(test_map, [1, 2, 3])