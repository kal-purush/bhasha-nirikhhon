#ice
n=int(input())
l=[]
while n>0:
    r=n%10
    if r%2==1:
        l.append(r)
    n=n//10
for i in range(len(l)-1,-1,-1):
    if i==len(l)-1:
        print(l[i],end="")
    else:
        print("",l[i],end="")