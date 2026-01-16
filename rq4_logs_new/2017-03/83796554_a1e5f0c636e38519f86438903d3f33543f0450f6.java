import java.util.*;

public class FibonacciHuge {
	public static long piasoperiod(long m)
	{
		long a=0,b=1,c=a+b;
		for(int i=0;i<m*m;i++)
		{
			c=(a+b)%m;
			a=b;
			b=c;
			if(a==0 && b==1) return i+1;
		}
		return m+1;
	}
    private static long getFibonacciHugeNaive(long n, long m) {
	    long remainder=n%piasoperiod(m);
	    long first=0;
	    long second=0;
	    long res=remainder;
	    for(int i=1;i<remainder;i++)
	    {
		    res=(first+second)%m;
		    first=second;
		    second=res;
	    }
	    return res%m;
    }
    
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        long n = scanner.nextLong();
        long m = scanner.nextLong();
        System.out.println(getFibonacciHugeNaive(n, m));
    }
}
