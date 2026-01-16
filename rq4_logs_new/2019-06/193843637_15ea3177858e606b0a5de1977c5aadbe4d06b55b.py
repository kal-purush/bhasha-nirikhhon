start,end=list(map(int,input().split()))
for counter in range(start+1,end):
  if counter%2==0:
    print(counter,end=" ")