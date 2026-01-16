import functools

def trace(show_types=False):
    def wrapper(f):
        @functools.wraps(f)
        def foo(*args):
            args_string = None
            if show_types:
                args_string = ', '.join(
                    '{}: {}'.format(arg, type(arg).__name__
                ) for arg in args)
            else:
                args_string = ', '.join(map(str, args))
            ret = f(*args)
            print('{}({}) -> {}'.format(f.__name__, args_string, ret))
            return ret
        return foo
    return wrapper