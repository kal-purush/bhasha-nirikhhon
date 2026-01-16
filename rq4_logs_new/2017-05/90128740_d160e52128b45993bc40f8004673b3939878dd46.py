def succ(x):
    return x + 1

successor = succ
del succ
print(successor(10))


# functions inside functions
def f1():
    def g1():
        print("hi, it's me 'g'")
        print("Thanks for calling me")

    print("This is the function 'f'")
    print("I am calling 'g' now")
    g1()


def temperature(t):
    def celsius2fahrenheit(x):
        return 9 * x / 5 + 32

    result = "It's " + str(celsius2fahrenheit(t)) + " degrees!"
    return result


# functions as parameters
def g():
    print("Hi, it's me 'g'")
    print("Thanks for calling me")


def f(func):
    print("Hi, it's me 'f'")
    print("I will call 'func' now")
    func()
    print("func's real name is " + func.__name__)


# functions return functions
def f2(x):
    def g2(y):
        return y + x + 3
    return g2


# a simple decorator
def out_decorator(func):
    def function_wrapper(x):
        print("Before calling " + func.__name__)
        func(x)
        print("After calling " + func.__name__)

    return function_wrapper


# below execution sequence: foo = out_decorator(foo), then foo(x)
@out_decorator
def foo(x):
    print("Hi, foo has been called with " + str(x))

foo(42)  # means functions_wrapper(42)