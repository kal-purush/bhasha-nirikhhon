from functools import wraps
from time import time


def show_entry(show):
    def decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            if show:
                print('Entering {}.{}'.format(func.__module__, func.__name__))
            return func(*args, **kwargs)
        return func_wrapper
    return decorator


def show_exit(show):
    def decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            ret = func(*args, **kwargs)
            if show:
                print('Exiting {}.{}'.format(func.__module__, func.__name__))
            return ret
        return func_wrapper
    return decorator


def show_entry_and_exit(show):
    def decorator(func):
        func = show_entry(show)(func)
        func = show_exit(show)(func)

        @wraps(func)
        def func_wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return func_wrapper
    return decorator


def show_args(show):
    def decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            if show:
                print(' args: {}\n kwargs: {}'.format(args, kwargs))
            return func(*args, **kwargs)
        return func_wrapper
    return decorator


def show_return(show):
    def decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            ret = func(*args, **kwargs)
            if show:
                print(' returns: {} (type: {})'.format(ret, type(ret)))
            return ret
        return func_wrapper
    return decorator


def show_args_and_return(show):
    def decorator(func):
        func = show_args(show)(func)
        func = show_return(show)(func)

        @wraps(func)
        def func_wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return func_wrapper
    return decorator


def time_exec(show):
    def decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            t0 = time()
            ret = func(*args, **kwargs)
            t_exec = time() - t0
            if show:
                print(' execution time: {}ms'.format(t_exec*1000))
            return ret
        return func_wrapper
    return decorator


def function_call_debug(show):
    def decorator(func):
        func = show_exit(show)(func)
        func = time_exec(show)(func)
        func = show_return(show)(func)
        func = show_args(show)(func)
        func = show_entry(show)(func)

        @wraps(func)
        def func_wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return func_wrapper
    return decorator