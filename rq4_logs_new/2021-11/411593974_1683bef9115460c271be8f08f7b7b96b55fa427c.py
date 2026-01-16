import time
import functools

#1
def caching(timeout):
    """Декоратор кэширования результата"""
    def outer_wrapper(func):
        time_ref, value = 0, 0
        def wrapper(*args):
            nonlocal time_ref
            nonlocal value
            now = time.time()
            if now - time_ref < timeout:
                print(value)
            else:
                value = func(*args)
                print(value)
                time_ref = now
            
        return wrapper
    return outer_wrapper