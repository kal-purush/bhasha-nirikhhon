package com.bridgelabz.algorithmsprograms;
import java.util.Scanner;

public class FindNumber {

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		System.out.println("Select a number");
		System.out.print("Enter the upper range: ");
		int range = scan.nextInt();
		
		FindNumber number1 = new FindNumber();
		number1.findNumber(0, range);
	}
	public void findNumber(int low, int high) {	
		Scanner scan = new Scanner(System.in);
		int mid = (low + high) / 2;
		
		System.out.println("Enter the selected number:");
		System.out.println("1)Greater than "+mid);
		System.out.println("2)Equal to "+mid);
		System.out.println("3)Less than "+mid);
		int choice = scan.nextInt();
		
		switch(choice) {
		case 1:
			if(high - mid == 1) {
				findNumber(high, high);
				break;
			}
			findNumber(mid, high);
			break;
		case 2: 
			System.out.println("Found the desired number");
			return;
		case 3:
			findNumber(low, mid);
			break;
		default:
			System.out.println(" Please enter a valid input:");
		}
	}
}