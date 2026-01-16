class Solution {
    public int longestPalindrome(String s) {
        Set<Character> set = new HashSet<>();
        int longest=0;

        for(char c: s.toCharArray()){
            if(set.contains(c)){
                set.remove(c);
                longest = longest +2;
            }
            else{
                set.add(c);
            }
        }
        if(!set.isEmpty()){
            return longest +1;
        } 
        return longest;  
    }
}