#Function to check if number is prime
def isPrime(n):
    flag = 1
    if n==2:
        return True
    for i in range(2, n//2+1):
        if n%i==0:
            flag = 0
            return False
    if flag==1:
        return True

#Function to determine number of Prime factors for a given number
def numberOfPrimeFactors(n):
    if isPrime(n):
        return 1
    count=0
    for i in range(2, n//2+1): #'/' is floating point division. '//' is integer division
        if isPrime(i) and n%i==0:
            count=count+1
    return count

#Program to find a natural number that is smaller than N such that it gives highest remainder when divided by that number. If there is more than one such number, print the smallest number.

def highestRemainder(n):
    hr=0
    v=n
    for i in range(n-1, n//2, -1):
        r=n%i
        if r>hr:
            hr=r
            v=i
    print(v)
    return
highestRemainder(30)