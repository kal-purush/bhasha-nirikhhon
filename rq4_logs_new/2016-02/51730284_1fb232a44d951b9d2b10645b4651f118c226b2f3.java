class Solution{
  
  public static void main(String [] args){
    
    Solution soln = new Solution();
    
    System.out.println(soln.isPalin("abcba"));
  }
  
  private boolean isPalin(String inpString){
    
    if(null == inpString || inpString.length() == 0){
      return false;
    }
    
    
    int strLen = inpString.length();
        
    if(strLen == 1){
      return true;
    }
    
    for (int i=0;i<strLen/2;i++){
      if(inpString.charAt(i) != inpString.charAt(strLen-1-i)){
        return false;
      }
    }
    return true;
  }
}