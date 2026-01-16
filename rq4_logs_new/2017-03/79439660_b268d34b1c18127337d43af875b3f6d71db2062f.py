class Solution(object):

    def myLengthOfLongestSubstring(self, s):
        """
        :type s: str
        :rtype: int
        """
        if len(s) <= 1:
            return len(s)
        else:
            repeated_flag = False
            result = s[0]
            result_len1 = 1
            result_len2 = 0
            repeated_index = 0
            for i in range(1, len(s)):
                # check whether char is repeated
                for j in range(len(result)):
                    if s[i] == result[j]:
                        repeated_index = j + 1
                        repeated_flag = True
                        break
                if repeated_flag:
                    break
                else:
                    result += s[i]
            if repeated_flag:
                result_len2 = self.lengthOfLongestSubstring(s[repeated_index:])
            result_len1 = len(result)
            if result_len1 >= result_len2:
                return result_len1
            else:
                return result_len2

    def lengthOfLongestSubstring(self, s):
        start = maxLength = 0
        usedChar = {}

        for i in range(len(s)):
            if s[i] in usedChar and start <= usedChar[s[i]]:
                start = usedChar[s[i]] + 1
            else:
                maxLength = max(maxLength, i - start + 1)

            usedChar[s[i]] = i

        return maxLength


if __name__ == '__main__':
    solution = Solution()
    print(solution.lengthOfLongestSubstring('pwwkew'))