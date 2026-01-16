package javaProject_Assignment1;
import java.util.Scanner;
public class LogicalBitwise {

	public static void main(String[] args) {
		byte a=0;
		byte b=0;
		boolean c = false;
		boolean d = false;
		
		System.out.println("Enter Numbers ");
		Scanner sc = new Scanner(System.in);

		a = sc.nextByte();
		b = sc.nextByte();
		if(a==1)
			c=true;
		if(b==1)
			d=true;
		if(a== 0 || a==1)
		{
		System.out.println("Bitwise Number is "+ (a & b));		
			
		 System.out.println("Logical Number is " + (c&&d));
		}
		else
		{
			System.out.println("Please enter 0 or 1");			
		}
		sc.close();

	}

}