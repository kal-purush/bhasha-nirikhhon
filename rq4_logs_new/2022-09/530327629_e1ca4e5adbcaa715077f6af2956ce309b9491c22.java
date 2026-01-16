package Questions;
//https://leetcode.com/problems/longest-common-prefix/
public class LongestCommonPrefix {
    public static void main(String[] args) {
        String[] arr={"flower","flow","flight"};
        System.out.println(longestCommonPrefix(arr));
    }
    public static String longestCommonPrefix(String[] strs) {
        String ans="";
        String compare=strs[0];
         int start=0;int end =compare.length()-1;
         while(start<=end){
             for(String s:strs){
                 if(start<= s.length()-1 && compare.charAt(start)==(s.charAt(start))){
                   continue;
                   
                 }else{
                   return ans;
                 }
             }
             ans+=compare.charAt(start);
             start++;
         }
         return ans;
    }}
