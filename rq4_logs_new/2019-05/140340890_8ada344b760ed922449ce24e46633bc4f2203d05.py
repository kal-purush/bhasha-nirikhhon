class Solution:
    def divide(self, dividend, divisor):
        """
        :type dividend: int
        :type divisor: int
        :rtype: int
        """
        result = 0
        sign = ((dividend > 0) - (dividend < 0)) * ((divisor > 0) - (divisor < 0))
        if dividend > ((2**31)-1) or divisor > (2**31)-1 or dividend < ((-2**31)-1) or divisor < ((-2**31)-1):
            return (2**31)-1
        dividend = -dividend if dividend < 0 else dividend
        divisor = -divisor if divisor < 0 else divisor
        if divisor == 1 or divisor == -1:
            return  sign * dividend if (sign * dividend) < ((2**31)-1) and (sign * dividend)> ((-2**31)-1) else (2**31)-1
        while dividend >= divisor:
            temp = divisor
            multiple = 1
            while dividend >= temp << 1:
                temp <<= 1
                multiple <<= 1
            dividend -= temp
            result += multiple
        return sign*result


solved = Solution()
print(solved.divide(-2147483648, -1))