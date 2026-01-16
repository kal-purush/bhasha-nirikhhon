
public class Ex1 {
	
	 static void fibonacciAverage(int n) {
		int t1,t2,t;
		double sum;
		t1 = 1;
		t2 = 1;
		sum = t1+t2;
		System.out.println("The first 20 Fibonacci numbers are: ");
		System.out.print(t1+" ");
		System.out.print(t2+" ");
		for (int i=2;i<n;i++) {
			t = t1+t2;
			sum+=t;
			t1 = t2;
			t2 = t;
			System.out.print(t+" ");
			
		}
		System.out.println("\nThe average is "+(sum/n)+".");
	}
	
	
	public static void main(String[] args) {
		int n;
		n= Integer.parseInt(args[0]);
		fibonacciAverage(n);
		
	}

}