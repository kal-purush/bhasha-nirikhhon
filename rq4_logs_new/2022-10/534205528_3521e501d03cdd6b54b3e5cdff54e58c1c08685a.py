s = []
for number in range(2):
	number = int(input())
	s.append(number)
	
def moto(arr):
	summa = 0
	for number in arr:
		summa = summa + number
	return(summa)
print(moto(s))
	
	
