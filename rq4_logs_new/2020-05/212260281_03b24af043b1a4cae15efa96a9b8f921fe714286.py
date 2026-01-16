# Given an array of strings, group anagrams together.

# EXAMPLE
# Input: ["eat", "tea", "tan", "ate", "nat", "bat"],
# Output:
# [
#   ["ate","eat","tea"],
#   ["nat","tan"],
#   ["bat"]
# ]

# Assumptions
# all inputs are lowercase
# order of input does not matter

class Solution:
    def groupAnagrams(self, strs: List[str]) -> List[List[str]]:
        '''
        Time: O(N * M log M), N is the strings in strs,
                              M is the char in the string
        Space: O(NM), dict grows as the num words grows total info content
        '''
        # create dictionary of a list
        table = {}

        for word in strs:
            # sort the word
            # use tuple for immutable object as key in dict
            key = tuple(sorted(word))

            # when the key is not found add [] + [word] = [word]
            # else add current value + [word] = [...,word]
            table[key] = table.get(key,[]) + [word]

        return table.values()