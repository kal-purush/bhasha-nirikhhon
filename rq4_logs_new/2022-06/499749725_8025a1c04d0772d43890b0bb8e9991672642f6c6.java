package study.assignment2;

import java.util.Scanner;

public class conversionUC_LC {
    /*Q3.Input string from user and convert 
uppercase letter to small case and small case to uppercase.
*/
	public static void main(String[] args) 
	{
		System.out.println("Enter String");
		Scanner sc = new Scanner(System.in);
		String s = sc.next();
		System.out.println(s);
	    String str="";
	 
		for(int i=0;i<s.length();i++)
		{
			if(Character.isLowerCase(s.charAt(i)))   //islowercase ...check the character is in lowercase
			{
				//Character.isUpperCase(s.charAt(i));
				str=str+Character.toUpperCase(s.charAt(i));    ////touppercase ...change the letter in uppercase
				
				
				
			}
			else
			{
				str=str+Character.toLowerCase(s.charAt(i));    //boolean function.....they only return true or false;
				
			}
		}
       System.out.println(str);
	}

}