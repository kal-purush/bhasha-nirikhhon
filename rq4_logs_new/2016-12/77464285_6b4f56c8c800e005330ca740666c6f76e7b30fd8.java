package classOne;

public class Operators {

	public static void main(String[] args) {
		/*
		 * Write a program to print the result of the following expressions provided the integer variable a is
			20 and b is 10.
			int b= a-- - --a;
			int c=a--;
			int d=a>>2;
			int e=a&b; 
		 * 
		 */
			
		int a = 20;
		//int b = 10;
		int b = a-- - --a;
		
		//a--=a-1 --a = 
		System.out.println("--a is " + --a);
		//System.out.println("--a is " + a--);
		System.out.println("Value of a-- - --a: " + b);
		int c = a--;
		System.out.println(" value of a-- is " + c);
		a=20;
		int d = a>>2;
		System.out.println("value of a>>2 is: " + d);
		b=10;
		int e = a&b;
		System.out.println(" value of a&b is: "+ e);
		
			

	}

}