import sys

def foo():
    friends = ["Bob", "Tom"]
    for f in friends:
        print("Hi {}!".format(f))
    return len(friends)


def tracefoo(target_function):
    def tracefunc(frame, event, arg):
        if frame.f_code.co_name != '_ag' and arg!=None:
            print("function:", frame.f_code.co_name, ", local vars:", [*frame.f_locals])
        return tracefunc
    sys.settrace(tracefunc)
    target_function()
    return

tracefoo(foo)