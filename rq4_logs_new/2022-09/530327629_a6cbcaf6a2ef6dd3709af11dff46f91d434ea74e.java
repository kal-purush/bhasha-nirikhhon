class Solution {
//https://leetcode.com/problems/check-if-binary-string-has-at-most-one-segment-of-ones/
    public boolean checkOnesSegment(String s) {
        boolean foundOne=false;boolean gap=false;
     for(int i=0;i<s.length();i++){
         if(s.charAt(i)=='1'){
             foundOne=true;
         }
         if(s.charAt(i)=='0' && foundOne==true){
             gap=true;
         }
         if(gap==true && s.charAt(i)=='1'){
             return false;
         }
     }
        return true;
    }
}