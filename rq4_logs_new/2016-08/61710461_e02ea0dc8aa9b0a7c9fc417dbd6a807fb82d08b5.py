n = input()
mod = 1000007
x1 = n / 2
x2 = n / 2 + 1
ans = 0
ans = max(ans, -(x1 * x1) + (x1 * n) + n)
ans = max(ans, -(x2 * x2) + (x2 * n) + n)
print ans % mod