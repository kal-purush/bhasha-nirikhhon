import math

print("-------------number1-------------")

def tg(a, b):
	phi = b/a
	if a >= 0:
		return math.atan(phi)
	else:
		return math.atan(phi * -1)
			
x1, x2 = map(float, input("Введите координаты через пробел: ").split())
y1, y2 = map(float, input("Введите координаты через пробел: ").split())
z1, z2 = map(float, input("Введите координаты через пробел: ").split())
print("Максимальный угол: ", min(tg(x1, x2), tg(y1, y2), tg(z1, z2)))

print("-------------number2-------------")

def prov(n):
	for i in range(1, n+1):
		count = 0
		a = bin(i)[2:]
		for j in range(len(a)//2):
			if a[j] == a[j*(-1) - 1]:
				count += 1
		if count == len(a)//2:
			print(i, a)

gran = int(input("Введите граничащее число: "))
prov(gran)

