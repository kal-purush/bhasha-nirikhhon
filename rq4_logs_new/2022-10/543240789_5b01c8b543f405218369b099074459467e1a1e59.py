# First we write the code using for loop.

def fibonacci(n):
    prev=0
    next=1
    while next<n:
        print(prev)
        next = next + prev
        prev = prev + next
        print(next)
        
# Now we write the code for recurtion.
# Very unoptimized code

def fibonacci_r(n,prev=0, next=0):
    if prev == 0:
        next = 1
        print(prev)
        print(next)
        next = 2
        prev = 1
    if prev < n+1 and next > n+1:
        print(prev)
        return None
    elif prev > n+1:
        return None
    else:
        print(prev)
        print(next)
        return fibonacci_r(n,prev+next, next+(prev+next))

def a(n=1):
    print(n)
    if n>5:
        return None
    return a(n+1)

# New optimized code using recursion

def fibonacci_ro(n,prev=0,new=1):
    if new>n:
        return None
    else:
        print(new)
    return fibonacci_ro(n,new, prev+new)