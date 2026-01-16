

listtimes = range(1,5)
listletter = ['x','y','z']
print([i*j for i in listletter for j in listtimes])  # Output : ['x', 'xx', 'xxx', 'xxxx', 'y', 'yy', 'yyy', 'yyyy', 'z', 'zz', 'zzz', 'zzzz']

listtimesouter = range(1,3)
listtimesinner = range(1,3)
listletter = ['x','y','z']
print([k*j*i for i in listtimesouter for j in listtimesinner for k in listletter])  # Output : ['x', 'y', 'z', 'xx', 'yy', 'zz', 'xx', 'yy', 'zz', 'xxxx', 'yyyy', 'zzzz']

listouter=range(2,5)
listinner=range(3)
print([[i+j] for i in listinner for j in listouter])  #Output :  [[2], [3], [4], [3], [4], [5], [4], [5], [6]]

listouter=[1,2,3]
listinner=[1,2,3]
print([(j,i) for i in listouter for j in listinner])  #Output :  [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (3, 2), (1, 3), (2, 3), (3, 3)]