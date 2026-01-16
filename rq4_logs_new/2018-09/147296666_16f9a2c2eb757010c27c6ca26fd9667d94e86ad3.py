from boa.interop.System.Runtime import Log
from boa.interop.System.Runtime import Notify
from boa.interop.System.Runtime import Serialize
from boa.interop.System.Runtime import Deserialize
from boa.interop.System.Storage import Put
from boa.interop.System.Storage import Get
from boa.interop.System.Storage import GetContext

ctx = GetContext()


def Main(operation ,args):
    if operation == 'save':
        return save()
    if operation == 'get':
        return get()
    return False

def save():
    #a = [1, 2, 3, 4]
    c = {'a':1,'b':2}

    b = Serialize(c)

    Put(ctx,'test',b)
    Notify('saved')
    return True

def get():
    b = Get(ctx, 'test')
    a = Deserialize(b)
    Log(a['a'])
    Notify(a['b'])