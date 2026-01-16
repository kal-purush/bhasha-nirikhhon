res = dict()
key = 0
for num in range(0,255):
    key = ( (((num << 5) | (num >> 3)) ^ 111) & 255 )
    res.setdefault(key, [])
    res[key].append(chr(num))
#print(res)
flag = [233, 129, 9, 5, 130, 194, 195, 39, 75, 229]
for a in flag:
    if a in res:
        print(a,res[a])
        