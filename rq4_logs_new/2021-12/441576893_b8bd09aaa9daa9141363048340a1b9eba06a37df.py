class Solution:
    def letterCombinations(self, digits: str) -> List[str]:
        if digits == "":
            return []
        
        dictionary = {
            "2": "abc",
            "3": "def",
            "4": "ghi",
            "5": "jkl",
            "6": "mno",
            "7": "pqrs",
            "8": "tuv",
            "9": "wxyz"
        }
        cursor = 0
        combinations = []
        
        def combine(letters, cursor):
            if len(digits) == cursor:
                combinations.append(letters)
                return;
            
            for char in dictionary[digits[cursor]]:
                combine(letters + char, cursor + 1)

        combine("", cursor)
        return combinations