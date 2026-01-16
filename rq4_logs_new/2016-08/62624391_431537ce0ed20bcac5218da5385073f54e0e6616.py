"""
module demonstrates the concepts of List Comprehension and Set Comprehension.
"""

list1 = []

for i in range(1,20):
    if i%2 == 0:
        list1.append(i)
"""
Above piece of code is used to form a list of values in a traditional way.
Let's look the same thing using List Comprehension.
"""

list2 = [i for i in range(1,20) if i%2 == 0]

print("LIST1 : ", list1)
print("LIST2 : ", list2)
"""
Both the lists results is same content as shown below :
OUTPUT :
LIST1 :  [2, 4, 6, 8, 10, 12, 14, 16, 18]
LIST2 :  [2, 4, 6, 8, 10, 12, 14, 16, 18]
"""

coord = [(x,y) for x in range(10) if x%2 == 0 for y in range(10) if y%3 ==0]
print(coord)

"""
OUTPUT:
[(0, 0), (0, 3), (0, 6), (0, 9), (2, 0), (2, 3), (2, 6), (2, 9), (4, 0), (4, 3),
(4, 6), (4, 9), (6, 0), (6, 3), (6, 6), (6, 9), (8, 0), (8, 3), (8, 6), (8, 9)]
"""

num = [j for i in range(2,8) for j in range(i*2,25,i)]
primes = [p for p in range(2,25) if p not in num]
print(primes)

"""
OUTPUT:
[2, 3, 5, 7, 11, 13, 17, 19, 23]

But if we look at the list "num" = [4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 6,
9, 12, 15, 18, 21, 24, 8, 12, 16, 20, 24, 10, 15, 20, 12, 18, 24, 14, 21].
There are duplicate entries like 6, 12, 18 and so on... In order to eliminate
those duplicate entries we can use the concept of Set Comprehension.
"""

num = {j for i in range(2,8) for j in range(i*2,25,i)}
primes = {p for p in range(2,25) if p not in num}

print("NUM SET : ", num)
print("PRIMES : ",primes)

"""
OUTPUT:
NUM SET :  {4, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 22, 24}
PRIMES :  {2, 3, 5, 7, 11, 13, 17, 19, 23}
"""


















































