print("Hello World")
# Will print the whole song with dear Emaline in the right place
print(list(map(lambda x: "Happy Birthday to" + (" you" if x !=2 else " dear Emaline"), range(4))))
#Will double the value of x up to 2 times 24
print(list(map(lambda x: x*2, range(24))))
# Find the minimum in the list
print(min([14, 35, -8, 24, 22]))
# Find the maximum in the list
print(max([14, 35, -8, 24, 22]))
# Will find the prime numbers. **Raising something to a power
n=50; print sorted(set(range(2, n+1)).difference(set((p * f)for p in range(2, int(n**0.5)+2)for f in range(2,n/p+1))))
# Counts by 3 from 0 to 20
for i in range(0,20,3):print(i)
# Prints a random number
import random
for i in range(10):
  diceRoll = random.randint(1,11)
print(diceRoll)
# Like a while loop
i=1
while i<=70:
  i+=2
  print(i)
# Will give you a poem about Python
import this