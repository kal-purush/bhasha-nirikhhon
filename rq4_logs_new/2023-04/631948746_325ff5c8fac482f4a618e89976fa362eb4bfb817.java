package labpractice1;

import java.util.Scanner;

public class SeokHoonKim_2022311519 {
	static Scanner sc = new Scanner(System.in); 
	public static void main(String[] args) 
	
	{
		int n = sc.nextInt(); 
		
		
		
		String result = ""; 
		for(int i=n; i>0; i/=2) {
			result = String.valueOf(i%2) + result;
		}
		System.out.println("b "+result);
		
		
		String two = "";
		for(int i=n; i>0; i/=8) {
			two = String.valueOf(i%8) + two;
		}
		System.out.println("o "+two);
		
		
		String three = "";
		for(int i=n; i>0; i/=16) {
		    if(i%16<10)
			    three = String.valueOf(i%16) + three;
			else
			    three= Integer.toHexString(i%16)+three;
		}
		System.out.println("h "+three);
		
	
	
	}
}