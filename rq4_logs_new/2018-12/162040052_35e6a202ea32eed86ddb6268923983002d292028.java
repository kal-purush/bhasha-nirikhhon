

import java.util.Scanner;

public class Ejercicio1 { 	//1.Hacer un programa que lea por teclado un n�mero entero y nos diga si el n�mero es positivo,	negativo o cero.

	public static void main(String[] args) {
		
		Scanner keyboard = new Scanner(System.in);
		int x;
		System.out.print("Introduce un n�mero: ");
		x=keyboard.nextInt();
		keyboard.close();
		
		if(x>=1)
			System.out.println("El n�mero es positivo");
		else if(x==0)
			System.out.println("El n�mero es 0");
		else if(x<=-1)
			System.out.println("El n�mero es negativo");
		
		
	}
}