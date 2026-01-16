/*
        Given a column title as appear in an Excel sheet, return its corresponding column number.

        For example:
        A -> 1
        B -> 2
        C -> 3
        ...
        Z -> 26
        AA -> 27
        AB -> 28 
        ...

        Example 1:
        Input: "A"
        Output: 1

        Example 2:
        Input: "AB"
        Output: 28

        Example 3:
        Input: "ZY"
        Output: 701
        

        Constraints:
        1 <= s.length <= 7
        s consists only of uppercase English letters.
        s is between "A" and "FXSHRXW".

*/
class Solution {
        public int titleToNumber(String s) {
                s           = s.toLowerCase();
                char[] arr  = s.toCharArray();
                if(arr.length == 0){
                        return 0;
                }
                if(arr.length == 1){
                        return arr[0] - 'a' + 1;
                }
                int excelNumber = 0;
                for(int i = 0; i < arr.length; i++){
                        excelNumber = (arr[i] - 'a' + 1) + excelNumber * 26;
                }
                return excelNumber;
        }
}