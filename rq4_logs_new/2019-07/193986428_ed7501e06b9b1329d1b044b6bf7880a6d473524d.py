class Solution(object):
    def fizzBuzz(self, n):
        """
        :type n: int
        :rtype: List[str]
        """
        result = []
        strBuild = ''
        print('hi')
        for i in range(1, n + 1):
            print(i)
            if (i % 3 == 0):
                strBuild = 'Fizz'
                if (i % 5 == 0):
                    strBuild += 'Buzz'
            if (i % 5 == 0):
                strBuild = 'Buzz'

            if (strBuild != ''):
                result.append(strBuild)
            else:
                result.append(str(i))

        return result


obj = Solution()
print(obj.fizzBuzz(15))