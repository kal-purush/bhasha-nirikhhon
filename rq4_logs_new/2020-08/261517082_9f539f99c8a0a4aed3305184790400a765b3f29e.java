/*
        A sentence S is given, composed of words separated by spaces. 
        Each word consists of lowercase and uppercase letters only.
        We would like to convert the sentence to "Goat Latin" (a made-up language similar to Pig Latin.)

        The rules of Goat Latin are as follows:

        If a word begins with a vowel (a, e, i, o, or u), append "ma" to the end of the word.
        For example, the word 'apple' becomes 'applema'.
        
        If a word begins with a consonant (i.e. not a vowel), remove the first letter and append it to the end, then add "ma".
        For example, the word "goat" becomes "oatgma".
        
        Add one letter 'a' to the end of each word per its word index in the sentence, starting with 1.
        For example, the first word gets "a" added to the end, the second word gets "aa" added to the end and so on.
        Return the final sentence representing the conversion from S to Goat Latin. 

        Example 1:
        Input: "I speak Goat Latin"
        Output: "Imaa peaksmaaa oatGmaaaa atinLmaaaaa"

        Example 2:
        Input: "The quick brown fox jumped over the lazy dog"
        Output: "heTmaa uickqmaaa rownbmaaaa oxfmaaaaa umpedjmaaaaaa overmaaaaaaa hetmaaaaaaaa azylmaaaaaaaaa ogdmaaaaaaaaaa"
        
        Notes:
        S contains only uppercase, lowercase and spaces. Exactly one space between each word.
        1 <= S.length <= 150.
*/
class Solution {
    
        Character[] arr = {'a','A','e','E','i','I','o','O','u','U'};
        Set<Character> set  = new HashSet<>(Arrays.asList(arr));
        
        public String toGoatLatin(String S) {
                StringBuilder str = new StringBuilder(); 
                S = S.trim();
                if(S.length() == 0){
                        return "";
                }
                String[] strs = S.split(" ");
                for(int i = 0; i < strs.length; i++){
                        StringBuilder sub = new StringBuilder(strs[i]);
                        if(!isVowel(strs[i], 0)){
                                rotate(sub);
                        }
                        sub.append("ma");
                        for(int j = 0; j <= i; j++){
                                sub.append("a");
                        }
                        if(i != 0){
                                str.append(" ");
                        }
                        str.append(sub.toString());
                }
                return str.toString();
        }
        
        private void rotate(StringBuilder sb){
                char ch = sb.charAt(0);
                sb.deleteCharAt(0);
                sb.append(ch);
        }
        
        private boolean isVowel(String str, int index){
                return set.contains(str.charAt(index));
        }
}