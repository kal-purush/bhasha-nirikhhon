class Solution {
  //https://leetcode.com/problems/long-pressed-name/
    public boolean isLongPressedName(String name, String typed) {
        int n=name.length();
        int t=typed.length();
        if(n>t){
            return false;
        }
     int i=0; int j=0;
        while(i<n && j<t){
               char namechar=name.charAt(i);
            char typedchar=typed.charAt(j);
            if(namechar != typedchar){
                return false;
            }
            int nameIndex=i+1;
            int typedIndex=j+1;
            while(nameIndex < n && name.charAt(nameIndex)==namechar){
                nameIndex++;
            }
            while(typedIndex < t && typed.charAt(typedIndex)==typedchar){
                typedIndex++;
            }
            if(typedIndex-j <nameIndex-i){
                return false;
            }
            i=nameIndex;
            j=typedIndex;
        }
        return i>=n && j>=t;
    }
}