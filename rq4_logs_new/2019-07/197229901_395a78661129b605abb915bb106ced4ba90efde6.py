p=input()
d=['a','e','i','o','u']
f=[]
n=[]
for i in p:
    if i in d:
        f.append(i)
for i in p:
    if i not in d:
        n.append(i)
v=(f+n)
print("".join(v))