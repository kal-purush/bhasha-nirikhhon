class Solution {
  //using two pointer method
    public String reverseWords(String s) {
        char[] charArray =s.toCharArray();
                int lastspacedIndex=-1;
        for(int startIndex=0;startIndex<=s.length();startIndex++){
    
            if(startIndex==s.length() || charArray[startIndex]==' '){
                int start=lastspacedIndex+1;
                int end=startIndex-1;
                while(start<end){
                    //swap
                    char temp =charArray[start];
                    charArray[start]=charArray[end];
                    charArray[end]=temp;
                    start++;
                    end--;
                }
                lastspacedIndex=startIndex;
            }
        }
        return new String(charArray);
    }
}