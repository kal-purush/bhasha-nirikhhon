import java.util.*;
import java.lang.*;

class UserSolution {
    public static int almost_palindrome() {
        Scanner sc = new Scanner(System.in);
        String str;
        StringBuffer sb = new StringBuffer();
        System.out.println("Enter a string: ");
        str = sc.next();
        sb.append(str);
        sb.reverse();
        String str1 = sb.toString();
        int i=0, count=0;
        while(str.length()!=i)
        {
            if(str.charAt(i)!=sb.charAt(i))
                System.out.println("Count: "+(++count));
            i++;
        }
        
        System.out.println("The ans is "+ count);
        
         return count;
    }
}

public class Main{
    public static void main(String args[])
    {
        UserSolution us = new UserSolution();
        us.almost_palindrome();
    }
}