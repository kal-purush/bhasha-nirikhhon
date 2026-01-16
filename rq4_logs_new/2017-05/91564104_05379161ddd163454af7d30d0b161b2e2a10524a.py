import queue
import threading

class Asyn(threading.Thread):

    def __init__(self, func, *args, **kwargs):
        super().__init__(target=func, args=args, kwargs=kwargs)
        self._return =None

    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)

    def join(self):
        self.join(self)
        return self._return


def make_async(func):

    def wrapper(*args, **kwargs):

        return Asyn(func, *args, **kwargs)

    return wrapper



