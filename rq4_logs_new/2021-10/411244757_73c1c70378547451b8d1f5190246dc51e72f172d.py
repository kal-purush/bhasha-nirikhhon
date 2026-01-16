class Solution:
    def solve(self, a, b):
        carry = 0
        digits = []

        if len(a) > len(b):
            b = b.zfill(len(a))

        if len(b) > len(a):
            a = a.zfill(len(b))

        i = len(a)-1

        while i >= 0:
            carry, digit = divmod(carry + int(a[i]) + int(b[i]), 10)
            digits.append(str(digit))
            i-=1

        if carry:
            digits.append("1")

        return"".join(reversed(digits))