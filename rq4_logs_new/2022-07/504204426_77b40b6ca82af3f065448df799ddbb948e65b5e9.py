a = [1,3,1,1,4,1,1,5,1,1,6,2,2]
count_max = 0
count_2 = 1
b = 0
a.sort()
for i in range(0,len(a)-1):
    if a[i+1] == a[i]:
        count_2 += 1
    elif a[i+1] != a[i]:
        count_max = max(count_max,count_2)
        if count_max == count_2:
            b = a[i]
        count_2 = 1
print(b)
count_max = max(count_max,count_2)
if count_max == count_2:
            b = a[i]
print(b)