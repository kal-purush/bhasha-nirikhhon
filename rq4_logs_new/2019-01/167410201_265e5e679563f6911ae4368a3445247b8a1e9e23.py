a = 'y = -12x - 12.11'
x = 2.5
index_x = a.index('x')
k = float(a[3:index_x])
b = a[index_x + 2:]
index_d = b.index('.')
if a[index_x + 2] == '+':
    b = int(b[1:index_d]) + int(b[index_d + 1:]) / (10 ** len(b[index_d + 1:]))
else:
    b = (int(b[1:index_d]) + int(b[index_d + 1:]) / (10 ** len(b[index_d + 1:])))*-1
y = k * x + b
print(y)
print()

#-----========-----

n=32
i=1
num_blok =0
floor_max=0
while num_blok<n:
   num_blok=num_blok+i*i
   floor_max+=i
   i+=1
i-=1
floor= floor_max - (num_blok-n)//i
num_left= n-(num_blok-(floor_max-floor+1)*i)
print(floor)
print(num_left)