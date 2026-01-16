/*
        Given a word, you need to judge whether the usage of capitals in it is right or not.

        We define the usage of capitals in a word to be right when one of the following cases holds:

        All letters in this word are capitals, like "USA".
        All letters in this word are not capitals, like "leetcode".
        Only the first letter in this word is capital, like "Google".
        Otherwise, we define that this word doesn't use capitals in a right way.
        

        Example 1:
        Input: "USA"
        Output: True
        

        Example 2:
        Input: "FlaG"
        Output: False
        

        Note: The input will be a non-empty word consisting of uppercase and lowercase latin letters.
*/
class Solution {
        public boolean detectCapitalUse(String word) {
                boolean firstCapital= false;
                boolean allCapital  =false;
                for(int i = 0 ; i < word.length(); i++){
                        if((int)word.charAt(i) > 64 && (int)word.charAt(i) < 91){
                                if(i == 0){
                                        firstCapital = true;
                                } else if(firstCapital && i == 1){
                                        allCapital = true;
                                } else if(!allCapital || !firstCapital){
                                        return false;
                                }
                        } else {
                                if(allCapital){
                                        return false;
                                }
                        }
                }
                return true;
        }
}