# key = list(input())  # 문자열을 리스트로 변환하여 저장
# a = len(key)
# password = [key]  # password 리스트에 key를 추가
# string = list(input())
# keys = ''
# myDict = {}
# for k in range(1,a+1):
#     myDict[k] = key[k-1]
# for i in range(0,len(string)-1,a):
#     password.append(string[i:i+a])
# s = len(string)//a
# for x in range(1,s+1):
#     for j in range(a):
#         myDict[j+1] += password[x][j]
# new_myDIct = sorted(myDict.values())
# result = [[ch for ch in char] for char in new_myDIct]
# realresult = ''
# for y in range(a):
#     for z in range(1,s+1):
#         realresult += result[y][z]
# print(realresult)


key = list(input())
realkey = key[:]
order = {}
for idx, elem in enumerate(realkey):
    order[idx] = elem
# print(order)

key.sort()
password = [key]
string = input()
a = len(key) #7
s = len(string)//a #8

for i in range(0,len(string)-1,s):
    password.append(list(string[i:i+s]))
# print(password)

myDict = {}
for idx, elem in enumerate(password[0]):
    myDict[idx] = elem
# print(myDict)

password = password[1:]
# print(password)

for x in range(a):
    for j in range(s):
        myDict[x] += password[x][j]
# print(myDict)

data = list(map(str, myDict.values()))
sorted_data = sorted(data, key=lambda x: list(order.keys())[list(order.values()).index(x[0])])
# print(sorted_data)

result = ''
for i in range(1,s+1):
    for elem in sorted_data:
        result +=elem[i]
print(result)
