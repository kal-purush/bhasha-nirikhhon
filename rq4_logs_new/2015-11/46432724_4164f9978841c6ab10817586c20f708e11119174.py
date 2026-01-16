import random

def compare(counter):
    match = 0
    b = random.randint(1,10)
    if counter == b:
        print("Match: ",counter, b)



counter = 1
for n in range(1,10):
    compare(counter)
    counter += 1