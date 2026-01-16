# Caleb
# Longest Substring Without Repeating Characters
# Given a string, find the length of the longest substring without repeating characters. 
# For example, the longest substring without repeating letters for "abcabcbb" is "abc", which the length is 3. 
# For "bbbbb" the longest substring is "b", with the length of 1.

class Solution(object):
    def lengthOfLongestSubstring(self, s):
        """
        :type s: str
        :rtype: int
        """
        count = 0
        max = 0
        str_list = list(s)
        char_dict = {}
        start = 0
            
        for idx, c in enumerate(str_list):
            if c not in char_dict:
                count += 1
                char_dict[c] = (count, idx)
            else:
                if char_dict[c][1] >= start:
                    start = char_dict[c][1] + 1
                    count = idx - start + 1
                else:
                    count += 1
                char_dict[c] = (count, idx)
                
            if count > max:
                    max = count
                
        if count > max:
            max = count
                    
        return max