name = "malaya kumar swain"
for i in name:
    print(i, end=" ")
print()

for i in range(len(name)-1,-1, -1):
    print(name[i], end=' ')
print()

for i in reversed(range(len(name))):
    print(name[i], end=' ')
print()
    
for i in reversed(range(5)):
    print(i)