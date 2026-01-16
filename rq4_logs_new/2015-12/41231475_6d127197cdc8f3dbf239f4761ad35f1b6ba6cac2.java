import java.io.*;
import java.util.*;
/*
  dic1
    dic2
    dic3
      pic.png
      note1.txt
      dic4
        dic6
          pic2.png
  dic5
*/
class Solution {
  
  public static void main(String[] args) {
    String input = "\tdic1\n\t\tdic2\n\t\t\tdic3\n\t\t\t\tpic.png";
    int output = findLongestLengthOfImage (input);
    System.out.println (output);
  }
  
  public static int findLongestLengthOfImage (String input) {
    // base case
    if (input == null || input.length() == 0 )
      return 0;
    
    // generic case
    String[] inputStr= input.split ("\n");
    int maxImageIndex = 0;
    String maxImageStr = "";
    int maxImageTabCount = -1;

    // find the deepest image string name (based on tab count)
    int c = 0;
    for (String str : inputStr) {
      if (str.endsWith (".jpg") || str.endsWith(".png") || str.endsWith (".jpeg")) {
        int currImageTabCount =  findMatches (str);
        // System.out.println (str.trim() + "--" + currImageTabCount);
        if (currImageTabCount > maxImageTabCount) {
          maxImageTabCount = currImageTabCount;
          maxImageStr = str;
          maxImageIndex = c;
        }
      }
      c++;
    }
    
    // edge case 1 - if images do not exist in the directory listing
    if (maxImageTabCount == -1)
      return 0;
    
    // now find the heirarchy of the string reverse up from the image location
    String output = "";
    for (int i=maxImageIndex; i >= 0; i--) {
      String str = inputStr[i];
      int cnt  = findMatches (str);
      if (cnt == maxImageTabCount) {
        output = "/" + str.trim() + output;
        // System.out.println (output + "--" + maxImageTabCount);
        maxImageTabCount--;
      }
    }
    /* // edge case 2 - improper heirarchy in the input string
    if (maxImageTabCount > 0)
      return 0; */
    
    System.out.println (output);
    return output.length();
  }
  
  // regex for pattern matching to count # of tabs
  public static int findMatches (String str) {
    java.util.regex.Pattern p = java.util.regex.Pattern.compile("\t");
    java.util.regex.Matcher m = p.matcher(str);
    int count = 0;
    while (m.find()){
      count +=1;
    }
    return count;
  }
}