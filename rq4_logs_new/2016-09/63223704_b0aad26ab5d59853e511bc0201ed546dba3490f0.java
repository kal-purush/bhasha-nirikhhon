import java.io.*;
import java.lang.*;
import java.util.*;
class Power
{
	public static void main(String ar[])
	{
	Scanner d=new Scanner(System.in);
	int i=0;
	int count=0;
	int num=d.nextInt();
	for( i=0;i<=num;i++)
		{        
			if(num==Math.pow(2,i))
                         {count=1;
			   break;}
		}
	if(count==1)
	  System.out.println("Power of two");
	else
	  System.out.println("Not Power of two");
	}
}