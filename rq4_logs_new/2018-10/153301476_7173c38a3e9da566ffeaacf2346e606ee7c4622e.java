package com.company;

import java.util.Scanner;

public class Main {
    public static int validarNumero(Scanner sc) {
        int numeroValidado;
        while (!sc.hasNextInt()) {
            System.out.print("¡Introduce un valor numérico entero! ");
            sc.next();
        }
        numeroValidado = sc.nextInt();
        return numeroValidado;
    }

    public static String comprobarNumero(int primerNumero, int segundoNumero) {
        String cadenaResultado;
        if (primerNumero > segundoNumero) {
            cadenaResultado = String.format("El número %d es mayor que el número %d.", primerNumero, segundoNumero);
        } else {
            if (segundoNumero > primerNumero) {
                cadenaResultado = String.format("El número %d es mayor que el número %d.", segundoNumero, primerNumero);
            } else {
                cadenaResultado = String.format("Los números %d y %d son iguales.", primerNumero, segundoNumero);
            }
        }
        return cadenaResultado;
    }

    public static void main(String[] args) {
        int primerNumero, segundoNumero;
        Scanner sc = new Scanner(System.in);
        System.out.println("¡Vamos a comprobar que número es mayor!");
        System.out.print("Introduce un primer número: ");
        primerNumero = validarNumero(sc);
        System.out.print("Introduce un segundo número: ");
        segundoNumero = validarNumero(sc);
        System.out.println(comprobarNumero(primerNumero, segundoNumero));
    }
}