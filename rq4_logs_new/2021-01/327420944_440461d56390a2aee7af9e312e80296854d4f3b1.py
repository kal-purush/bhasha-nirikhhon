#import radnom generator pack:

import random

#self-made module need to be in the same folder
import module1

#Giving random integers from 7 to 20:
print(random.randint(7,20))
print(random.randint(7,20))
print(random.randint(7,20))
print(random.randint(7,20))
print(f"{random.randint(7,20)}" + "\n")

c=module1.first
print(c)
print(module1.end)
print(random.randint(module1.first , module1.end))


random_float=round(random.random(),2)
print(random_float)

#random() does not have arguments. If I need greater random numbers then from 0.0000...-0.99999... we do it by using multiplication:
random_float2=random.random()*5  #for 0-4.9999
print(random_float2)

random_float3=random.random()*8  #for 0-7.9999
print(random_float3)