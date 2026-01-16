a = int(raw_input('Enter A: '))
b = int(raw_input('Enter B: '))
c = int(raw_input('Enter C: '))

if b == 2:
	r = a % c
elif b == 4:
	r = a + c
elif b == 8:
	r = a * c

if r % 4 == 0:
	result = r / 4
	print result
	print r
else:
	result = r % 4
	print result
	print r