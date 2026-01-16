from time import strftime as displaytime, localtime, sleep


# <HW12> Task1
def func_name_start(some_func):
    """Decorator function displays name of function which is decorated and start time of function operation"""

    def inner_func():
        print(f"Function '{some_func.__name__}' started at {displaytime('%H:%M:%S', localtime())}")
        some_func()

    return inner_func


@func_name_start
def my_function():
    print("This is my function")


@func_name_start
def my_function2():
    print("My second function")


if __name__ == "__main__":
    my_function()
    sleep(3)
    my_function2()