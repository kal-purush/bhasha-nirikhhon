"""
49. Group Anagrams

Given an array of strings `strs`, group the anagrams together.
You can return the answer in any order.
"""

"""
Example 1:
Input: strs = ["eat","tea","tan","ate","nat","bat"]
Output: [["bat"],["nat","tan"],["ate","eat","tea"]]

Explanation:
There is no string in strs that can be rearranged to form "bat".
The strings "nat" and "tan" are anagrams as they can be rearranged to form each other.
The strings "ate", "eat", and "tea" are anagrams as they can be rearranged to form each other.

Example 2:
Input: strs = [""]
Output: [[""]]

Example 3:
Input: strs = ["a"]
Output: [["a"]]
"""

class Solution:
    def groupAnagrams(self, strs: list[str]) -> list[list[str]]:
        hash_map = {}

        for i in strs:
            sorted_word_tuple = tuple(sorted(i))
            
            if sorted_word_tuple not in hash_map.keys():
                hash_map[sorted_word_tuple] = []

            hash_map[sorted_word_tuple].append(i)

        return list(hash_map.values())

sol = Solution()
print(sol.groupAnagrams(strs = ["eat","tea","tan","ate","nat","bat"]))