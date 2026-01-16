package assignment1;

import java.util.Scanner;

public class AnnieCountthenumberofdigits {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Write a Java program count the number of digits of the number using while loop.​

		Scanner scanner=new Scanner(System.in);
		System.out.println("Enter a valid number: ");
		int num=scanner.nextInt();
		scanner.close();
		int count=0;
		while(num!=0)
		{
			num=num/10;
			count++;
		}
		System.out.println("Number of digits in the integer : "+count);
		
	}

}