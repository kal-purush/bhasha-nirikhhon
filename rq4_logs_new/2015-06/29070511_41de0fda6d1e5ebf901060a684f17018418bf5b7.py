from types import MethodType

class Event(object):

    def __init__(self):
        self.__handlers = set()

    def __iadd__(self, handler):
        self.__handlers.add(handler)
        return self

    def __isub__(self, handler):
        self.__handlers.remove(handler)
        return self

    def __call__(self, *args, **kwargs):
        for handler in self.__handlers:
            if isinstance(handler, MethodType):
                handler(handler.__self__, *args, **kwargs)
            else:
                handler(*args, **kwargs)