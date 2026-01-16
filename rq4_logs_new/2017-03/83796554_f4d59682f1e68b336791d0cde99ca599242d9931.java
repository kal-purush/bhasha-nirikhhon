import java.util.*;

public class FibonacciSumLastDigitalpha {
/*public static long piasoperiod(int m)
	{
		long a=0,b=1,c=a+b;
		int p=0;
		for(int i=0;i<m*m;i++)
		{
			c=(a+b)%10;
			a=b;
			b=c;
			if(a==0 && b==1) 
			{
				p=i+1;
				break;


			}

		}
		System.out.println(p);
		return p;


	}
	
*/
   private static long getFibonacciSum(long n) {
		
		long sum=0;	
		long remainder=n%60;
		long first=0;
		long second=1;
		long res=remainder;
		for(int i=1;i<remainder+2;i++)
		{
			res=(first+second)%60;
			first=second;
			second=res;
		}
		sum=second-1;
		return sum%10;
	}
    
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        long n = scanner.nextLong();
        long s = getFibonacciSum(n);
        System.out.println(s);
    }
}
