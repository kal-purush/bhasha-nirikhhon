package Questions;

import java.util.Arrays;

public class ShuffleString {
      public static void main(String[] args) {
       String s ="codeleet";
      
        int[] indices = {4,5,6,7,0,2,1,3};
      
        System.out.println( restoreString(s,indices));
      }
      public static String restoreString(String s, int[] indices) {
        StringBuilder builder=new StringBuilder();
      char[] myarray=new char[s.length()];
      
        for (int i = 0; i < indices.length; i++) {
            myarray[indices[i]]=s.charAt(i);
        }
    
      for(char c:myarray){
        builder.append(c);
      }
      String ans=builder.toString();
        return ans;
    }
}