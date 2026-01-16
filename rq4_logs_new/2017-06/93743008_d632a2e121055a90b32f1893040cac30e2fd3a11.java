import java.io.*;
import java.util.*;
public class revers
{
public static void main(String args[])
{
int t=0;
Scanner s=new Scanner(System.in);
int n=s.nextInt();
int[] a=new int[n];
while(n>0)
{
t=t*10+n%10;
n=n/10;
}
System.out.println(t);
}
}