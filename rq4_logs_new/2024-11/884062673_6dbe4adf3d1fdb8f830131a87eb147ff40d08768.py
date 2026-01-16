a, b = map(int, input().split())
s = input()

flag = False
for i in range(len(s)):
    if i != a and not s[i].isdigit():
        flag = True
        break

if flag:
    print("No")
    exit()

if s[a] == '-':
    print("Yes")
elif len(s) != a + b + 1:
    print("No")
else:
    print("No")