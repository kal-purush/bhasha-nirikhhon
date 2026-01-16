# PE633 - Square Prime Factors II
import math


# Approach:
# 1 - Find list of primes less than or equal to N.
# 2 - Multiply each number in list by every other number in list.
# 3 - If squares are less than or equal to N, increment C sum.
# 4 - Any non-squares that are less or equal to the square root on N
#     are added to the list.
# 5 - Repeat from step 2, by multiplying new entries by full list.
# 6 - Stop when every product is greater than N.



#A simple sieve technique to create a single long list of all prime numbers below "limit".
#This is useful for generating a list of primes to iterate through. Not as efficient for
#generating a list of primes to find a specific value in.
def Sieve(limit):
    j = 2
    PrimeList = [True]*limit
    Primes = []

    #Set all of the composite number indices to False
    for i in range(2,limit):
        if PrimeList[i] == True:
            while i*j < limit:
                PrimeList[i*j] = False
                j = j+1

        j = 2
        
    #Create a single list containing all prime numbers under a given limit
    j = 1        
    for i in range(2, limit):
        if PrimeList[i] == True:
            Primes.append(i)
    return Primes


S
N = 10
rootN = math.floor(math.sqrt(N))







"""
numList = list(Sieve(N))
C_sum = 0
minProd = rootN
newIndex = 0

while(minProd <= N):
    newList = list()
    for i in range(newIndex,len(numList)):
        for j in numList:
            prod = i*j
            if(i == j)&(prod<=N):
                C_sum = C_sum + 1
            elif(prod<=rootN):
                newList.add(prod)
"""






