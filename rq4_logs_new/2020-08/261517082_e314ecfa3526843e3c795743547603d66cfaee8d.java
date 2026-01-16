/*
        Given a string which consists of lowercase or uppercase letters, 
        find the length of the longest palindromes that can be built with those letters.

        This is case sensitive, for example "Aa" is not considered a palindrome here.

        Note:
        Assume the length of given string will not exceed 1,010.

        Example:
        Input:
        "abccccdd"
        Output:
        7

        Explanation:
        One longest palindrome that can be built is "dccaccd", whose length is 7.
*/
class Solution {
        public int longestPalindrome(String s) {
                Map<Character, Integer> map = new HashMap();
                for(char ch : s.toCharArray()){
                        map.put(ch, map.getOrDefault(ch, 0) + 1);
                }
                int count = 0;
                for(Map.Entry<Character, Integer> entry : map.entrySet()){
                        if(entry.getValue() % 2 == 1){
                                count++;
                        }
                }
                return count == 0 ? s.length() : s.length() - count + 1;
        }
}