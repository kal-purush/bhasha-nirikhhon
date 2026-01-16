/*

Given a positive integer, return its corresponding column title as appear in an Excel sheet.

For example:

    1 -> A
    2 -> B
    3 -> C
    ...
    26 -> Z
    27 -> AA
    28 -> AB


*/



//Solution


public class Solution {
	public String convertToTitle(int a) {
	    
	    int rem;
	    char remChar='\0';
	    String result="";
	    while(a>=1){
	        rem=a%26;
	        if(rem>0)
	        remChar = (char)(rem+64);
	        else
	        remChar = 'Z';
	        result = remChar+result;
	        if(rem==0)
	        a=(a/26)-1;
	        else
	        a=a/26;
	    }
	    return result;
	}
}