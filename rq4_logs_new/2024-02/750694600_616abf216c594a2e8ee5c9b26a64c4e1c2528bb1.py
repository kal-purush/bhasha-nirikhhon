N=int(input())
index=[]
arr=[0]*1001
max_H=0
max_idx=0
result=0
for _ in range(N):
    idx,H=map(int,input().split())
    arr[idx]=H
    index.append(idx)
    if H>max_H:
        max_H=H
        max_idx=idx
index=sorted(index)
mul=arr[index[0]]
for i in range(index.index(max_idx)):
    result+=mul*(index[i+1]-index[i])
    if arr[index[i]]<arr[index[i+1]]:
        mul=arr[index[i+1]]
    else:
        mul=arr[index[i]]
for i in range(N-1,index.index(max_idx),-1):
    result+=mul*(index[i+1]-index[i])
    if arr[index[i]]<arr[index[i+1]]:
        mul=arr[index[i+1]]
    else:
        mul=arr[index[i]]
print(result)