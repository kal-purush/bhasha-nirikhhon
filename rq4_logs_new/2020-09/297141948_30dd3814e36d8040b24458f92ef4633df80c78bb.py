#
# Q: How to organize parameters for a set of objects, e.g. instrumentation?
# A: Defer initialization to an __init__ decorator.
#
class InitParams:
    arguments = {
            'MyClass' : {'bArg': True, 'fArg': [[1,0],[0,1]]}
            }
    def __init__(self, classname):
        self.arguments = __class__.arguments[classname]
    def __call__(self, initfn):
        def wrapper(other, **kwargs):
            for k in self.arguments:
                if k not in kwargs:
                    kwargs[k] = self.arguments[k]
            initfn(other, **kwargs)
        return wrapper

class InvalidClass:
    # @InitParams('InvalidClass') # Uncomment too see a KeyError
    def __init__(self):
        pass
class MyClass:
    @InitParams('MyClass')
    def __init__(self, bArg, fArg):
        print('bArg: {}, fArg: {}'.format(bArg, fArg))

if __name__ == "__main__":
    # Instantiate with default parameters
    f = MyClass()
    # Instantiate and override 'bArg' parameter
    f = MyClass(bArg = False)