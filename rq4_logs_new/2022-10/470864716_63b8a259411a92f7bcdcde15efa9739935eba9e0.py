path = input()

with open(path, "r") as f:
    a = f.read()
    
a = a.split('\n')
di = dict()
for i in a:
    gg = i.split('\t')
    if len(gg) <= 1:
        continue
    n = int(gg[0].split()[0])
    gg[0] = gg[0].split()[1]
    ggg = []
    for i in gg:
        if len(i) != 0:
            ggg.append(i)
    gg = ggg
    if n not in di:
        di[n] = [gg]
    else :
        di[n].append(gg)

new_di = dict()
for i in di:
    mi = float(di[i][0][-2])
    pos = di[i][0]
    for j in di[i]:
        val = float(j[-2])
        if mi > val:
            mi = val
            pos = j
    new_di[i] = pos
    
for i in new_di:
    for j in range(-5, 0):
        print(new_di[i][j], end=' ')
    print()