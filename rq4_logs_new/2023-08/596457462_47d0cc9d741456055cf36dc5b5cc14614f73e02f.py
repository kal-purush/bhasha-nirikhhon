class Solution:
    def myPow(self, x: float, n: int) -> float:
        if n < 1:
            x, n = 1 / x, -n
        res = 1
        while n > 0:
            if n % 2 == 0:
                x *= x
                n //= 2
                continue
            res *= x
            n -= 1
        return res