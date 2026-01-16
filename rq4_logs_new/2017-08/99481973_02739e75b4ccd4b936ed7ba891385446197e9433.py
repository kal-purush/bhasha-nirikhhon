supplies = ['pen','staplers', 'flower','binders']

for i in range(len(supplies)):
    print(str(i) + '' + supplies[i])

## check index number

# express number 0
print(supplies.index('pen'))

supplies.append('goods')

##append goods end of supplies
print(supplies)

supplies.insert(2, 'human')
##human insert index 2 to supplies
print(supplies)

supplies.remove('pen')

##remove pen from supplies
print(supplies)


numberList = [-7,23,11,32,5]
strList = ['apple','king','cook','test','burger']
numberList.sort()
strList.sort()
print(numberList)
print(strList)