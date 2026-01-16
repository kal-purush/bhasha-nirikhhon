print("Hello")


# add:
# 1. get two numbers 
# 2. figure out their total
# 3. give their total to the user

# named: add
# inputs: x and y
# outputs: the sum of x and y
def add(x, y):
  total = x + y
  return total

print(add(1, 2)) # gives 3
print(add(1, 2))
print(add(-10, 1000))

# calculate p(x) = x^2 + 2x + 4 
# p(1) = 1^2 + 2*1 + 4 = 1 + 2 + 4 = 7
# p(0) = 4
# inputs: any number x
# outputs: p(x)
# 1. Get x                                     3
# 2. Calculate and save (step2)    x^2         3^2 = 3*3 = 9
# 3. Calculate and save (step3)    2x          2*x = 6
# 4. Calculate and save (step4)    step2 + step3   x^2 + 2x + ???
# 5. Calculate and save 
def poly_add(x):
  step2 = x**2
  step3 = 2*x
  step4 = step2 + step3
  return step4 + 4

def poly_add2(x):
  return x**2 + 2*x + 4

print("expect 4 :" + str(poly_add(0)))
print("expect 7 :" + str(poly_add(1)))

# Homework:
# 1. Write an algorithm for getting dressed in the morning. Use enough
#    detail to make sure you are fully ready to go.
#
# 2. Install Atom
# 
# 3. Think about other algorithms in your life 
#    or specific things you wish you could do with a computer. 