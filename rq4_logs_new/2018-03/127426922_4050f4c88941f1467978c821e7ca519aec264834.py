import sys

def simpleArraySum(n, ar):
	jumlah = 0
	a = 0
	while a < n:
		jumlah += ar[a]
		a += 1 	

	return jumlah


n = int(input('masukan n: ').strip())
ar = list(map(int, input('masukan ar :').strip().split(' ')))

hasil = simpleArraySum(n, ar)
print(hasil)