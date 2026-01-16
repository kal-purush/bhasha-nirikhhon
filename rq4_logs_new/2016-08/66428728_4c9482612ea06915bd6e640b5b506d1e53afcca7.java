import java.util.*;
public class armstrong{

     public static void main(String []args){
       Scanner sc=new Scanner(System.in);
       int a=sc.nextInt();
       int s=0,b=0,c=a;
       while(a!=0)
       {
           b=a%10;
           s=s+b*b*b;
           a=a/10;
       }
       if(s==c)
       System.out.print("It is an armstrong number");
       else
       System.out.print("It is not an Armstrong number");
     }
}