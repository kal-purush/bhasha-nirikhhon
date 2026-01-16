package ejercicio_Tema3;

import java.util.Scanner;

public class Ejercicio10 {
	public static void main(String[] args) {
		Scanner s = new Scanner(System.in);
		Integer longitudArray = 0;
		Integer[] numeros;
		do {
			System.out.println("Introduzca la longitud del array, debe ser mayor que 2");
			longitudArray = s.nextInt();
			numeros = new Integer[longitudArray];
			if (longitudArray <= 2) {
				System.out.println("Longitud inferior/igual a 2");
			}
		} while (longitudArray <= 2);

		Integer x = 0;
		Integer x1 = 1;

		for (int i = 0; i < numeros.length-1; i++) {
			numeros[i] = x;
			numeros[i+1] = x1;
			x1 = x + x1;
			x = x1 - x;
		}
		for (int i = 0; i < numeros.length; i++) {
			System.out.println(numeros[i]);
		}
		s.close();
	}
}