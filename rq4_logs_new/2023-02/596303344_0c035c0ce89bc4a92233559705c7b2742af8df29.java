

//1. Realiza un método que reciba dos números y devuelva True si uno es múltiplo del otro.


package com.edu;

import java.util.Scanner;


public class ejercicio_1{
	public static void main(String[] args) {
		
		int num1=0;
		System.out.println("Introduce número: ");
		try (Scanner sc = new Scanner(System.in)) {
			num1= Integer.valueOf(sc.nextLine());
			System.out.println();
			
			int num2=0;
			System.out.println("Introduce otro número: ");
			num2= Integer.valueOf(sc.nextLine());

			
			System.out.println();
			if (num1 % num2==0 || num2 % num1==0) {
				System.out.println("¡Es True!");
				
			}else if (num1 % num2!=0 || num2 % num1!=0) {
				System.out.println("¡Es False!");
			}
		}		
	}
	
}