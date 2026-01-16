low = int(input())
up = int(input())
s = 0
for i in range(low,up+1):

	j = i*i
	temp = str(j)
	temp = list(temp)
	n = len(temp)
	if n%2 == 0:
		n = n//2
	else:
		n = n//2 + 1
	
	first = j//(10**(n))
	last = j%(10**(n))
	if(i == (first + last)):
		print(i , end=' ',sep=' ' )
		s = 1
if s == 0:
	print("INVALID RANGE")
	