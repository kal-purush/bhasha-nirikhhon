class Solution {
    public String longestCommonPrefix(String[] strs) {
        if(strs.length==0 || strs[0].length()==0){
            return "";
        }
     
            int min =Integer.MAX_VALUE;
         String ans="";
            for(String str:strs){
                min=Math.min(min,str.length());
            int start=0;   int end=min;
                while(start<=end){
                        int mid=(start+end)/2;
                    if(isCommonPrefix(strs,mid)){
                        start=mid+1;
                    }else{
                        end=mid-1;
                    }
                }
                ans=  strs[0].substring(0,(start+end)/2);
            }
        return ans;
        }
    boolean isCommonPrefix(String[] strs,int mid){
        String str1=strs[0].substring(0,mid);
        for(int i=1;i<strs.length;i++){
            if(!strs[i].startsWith(str1)){
                return false;
            }
        }
        return true;
    }
}