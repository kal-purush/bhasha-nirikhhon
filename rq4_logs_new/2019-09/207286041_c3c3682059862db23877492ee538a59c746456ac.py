a=[1,2,3,4,5,6,7,8]
b=0
c=0
for i in range(8):
    if(a[i]%2==0):
        b=b+a[i]
    if(a[i]%2!=0):
        c=c+a[i]
print(b)
print(c)
d=b-c
print(d)
        
        