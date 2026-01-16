print('hello')

def isum():
    isum=0
    for i in range(1,10):
        isum += i
        print(i)
    return isum

print(isum())