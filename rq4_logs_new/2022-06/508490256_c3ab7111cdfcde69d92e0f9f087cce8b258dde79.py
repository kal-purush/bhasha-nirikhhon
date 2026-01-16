import math
for i in range(2, 100):
    if all(i%j!=0 for j in range(2,math.floor(math.sqrt(i))+1)):
        print(i,end=' ')
print('')