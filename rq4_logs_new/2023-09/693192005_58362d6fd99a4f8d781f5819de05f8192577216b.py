# print(''.join(input("Введите числа через пробел:").split()[::2]))

# n = input("Введите числа через пробел:").split()
# print(max(n), n.index(max(n)))

# print(''.join(reversed(list(input("Введите числа: ")))))

# n = list(input("Введите числа: "))
# n[:-1:2], n[1::2] = n[1::2], n[:-1:2]
# print(n)

# n = list(input("Введите числа: "))
# print(n[-1:]+n[:-1])

# n = list(input("Введите числа: "))
# new_n = n.pop(2)
# print(''.join(n))

# n = input("Введите числа через пробел: ").split()
# n.insert(int(input("Введите индекс: ")), input("Введите число:"))
# print(''.join(n))

n, i = input("Введите числа через пробел: ").split(), int(input("Введите значение индекса "))
print(n[+i-1:] + n[:+i-1])