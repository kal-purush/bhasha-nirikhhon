/*
        Given a string, determine if it is a palindrome, 
        considering only alphanumeric characters and ignoring cases.

        Note: For the purpose of this problem, we define empty string as valid palindrome.

        Example 1:
        Input: "A man, a plan, a canal: Panama"
        Output: true

        Example 2:
        Input: "race a car"
        Output: false
        
        Constraints:
        s consists only of printable ASCII characters.
*/
class Solution {
        public boolean isPalindrome(String s) {
                s          = s.toLowerCase();
                char[] arr = s.toCharArray();
                int first,last;
                for(int i = 0, j = arr.length - 1 ; i < j;){
                        first = (int)arr[i];
                        last  = (int)arr[j];
                        if((first < 97 || first > 122) && (first < 48 || first > 57)) {
                                i++;
                                continue;
                        }
                        if((last < 97 || last > 122) && (last < 48 || last > 57)){
                                j--;
                                continue;
                        }
                        
                        if(first != last){
                                return false;
                        }
                        i++;
                        j--;
                }
                return true;
        }
}