n = int(input())

for _ in range(n):
    str = input().split()
    for i in range(len(str)):
        str[i] = str[i][1:] + (str[i][0]).lower() + 'ay'

    str[0] = (str[0][0]).upper() + str[0][1:]
    print(" ".join(str))