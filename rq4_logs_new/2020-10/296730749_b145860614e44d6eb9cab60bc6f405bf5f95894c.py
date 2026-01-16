import random

def sarcastify(your_string):
    return "".join(x.upper() if random.randint(0,1) else x for x in your_string)

print(sarcastify("well what are you going to do about it?"))