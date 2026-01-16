from timer import timer


class lazy_object:
    def __init__(self, callable, *args, **kw):
        self.__dict__['callable'] = callable
        self.__dict__['args'] = args
        self.__dict__['kw'] = kw
        self.__dict__['obj'] = None

    def initObj(self):
        if self.obj is None:
            self.__dict__['obj'] = self.callable(*self.args, **self.kw)

    def __getattr__(self, name):
        self.initObj()
        return getattr(self.obj, name)

    def __setattr__(self, name, value):
        if name == 'reset' and value == 1:
            self.__dict__['obj'] = None
        else:
            self.initObj()
            setattr(self.obj, name, value)

    def __len__(self):
        self.initObj()
        return len(self.obj)

    def __getitem__(self, idx):
        self.initObj()
        return self.obj[idx]


class A:
    def __init__(self, num_elem):
        self.attr1 = list(range(num_elem))
        self.reset = 0


a = lazy_object(A, num_elem=10 ** 7)

with timer('Elapsed: {}ms'):
    type(a.attr1)  # 4202.2ms

with timer('Elapsed: {}ms'):
    type(a.attr1)  # 0.008ms

a.reset = 1

with timer('Elapsed: {}ms'):
    type(a.attr1)  # 3542.5ms

with timer('Elapsed: {}ms'):
    type(a.attr1)  # 0.007ms