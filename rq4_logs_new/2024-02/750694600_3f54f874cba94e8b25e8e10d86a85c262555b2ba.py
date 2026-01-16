def rev(n) : 
    p = [5,3,4]
    if n in p:
        return p.index(n)
    else : 
        return p[n]

N = int(input())
ds = []
for i in range(N):
    d = list(map(int,input().split()))
    ds.append(d)

max_sum = 0

for i in range(6):
    st = ds[0][i]
    end = ds[0][rev(i)]
    loc_sum = max([ el for el in ds[0] if el != st and el != end])
    for j in range(1,len(ds)):
        st = ds[j][ds[j].index(end)]
        end = ds[j][rev(ds[j].index(end))]
        loc_sum+=max([ el for el in ds[0] if el != st and el != end])
    if max_sum<loc_sum:
        max_sum = loc_sum
        
print(max_sum)