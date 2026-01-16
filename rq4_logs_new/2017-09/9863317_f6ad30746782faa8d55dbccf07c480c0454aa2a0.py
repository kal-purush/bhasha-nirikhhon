import prime

i =1 
large = 600851475142
while i < large:
	if large % i == 0:
		if prime.isPrime(i):
			print(i)
	i +=1