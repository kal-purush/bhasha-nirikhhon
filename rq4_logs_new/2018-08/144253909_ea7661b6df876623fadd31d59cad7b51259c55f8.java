
import java.lang.*;
import java.io.*;
import java.util.*;
class ReverseString
{
    public static void main(String[] args)
    {
       Scanner sc=new Scanner(System.in);
       String s=sc.next();
        StringBuilder input1 = new StringBuilder();
        input1.append(s);
        input1 = input1.reverse();
        System.out.println(input1);
    	
    }
}