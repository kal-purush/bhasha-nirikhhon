initial_Fib = [1,2]
even_Fib = []
index = 1

while initial_Fib[index] < 4000000:
    next = initial_Fib[index] + initial_Fib[index - 1]
    initial_Fib.append(next)
    if initial_Fib[index] % 2 == 0:
        even_Fib.append(initial_Fib[index])
    index += 1

print(str(even_Fib))
finalAns = sum(even_Fib)
print(finalAns)