n,k=map(int,input().split())
k -= 1
s = [int(i) for i in input().split()]
print(len([i for i in s if i >= s[k] if i > 0]))