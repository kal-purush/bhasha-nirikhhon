n="210022"
b=3
def numberToBase(n, b):
    if n == 0:
        return [0]
    digits = []
    while n:
        digits.append(int(n % b))
        n //= b
    return digits[::-1]
def convert(list): 
    s = [str(i) for i in list] 
    res = int("".join(s)) 
    return(res) 
l=[]
while True:
	k=len(n)
	y1=''.join(sorted(n))
	x1=y1[::-1]
	x=int(x1,b)
	y=int(y1,b)
	z=x-y
	z=convert(numberToBase(z,b))
	z1=str(z)
	z1.zfill(k)
	#print(z1)
	if l.count(z1)>0:
		h=len(l)-l.index(z1)
		break
	l.append(z1)
	n=z1
print(h)
