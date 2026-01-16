package edu.cesarivan666.reto3.ui;

import edu.cesarivan666.reto3.process.ConversorNumerico;

import java.util.Scanner;

public class CLI {
    public static void showmenu() {

        Scanner sc = new Scanner(System.in);
        int choice;
        boolean exit = false;
        String number;
        while (!exit) {
            System.out.println("Menu de conversiones:");
            System.out.println("1) Convertir de decimal a binario");
            System.out.println("2) Convertir de decimal a octal");
            System.out.println("3) Convertir de decimal a hexadecimal");
            System.out.println("4) Convertir de binario a decimal");
            System.out.println("5) Convertir de octal a decimal");
            System.out.println("6) Convertir de hexadecimal a decimal");
            System.out.println("7) Salir");
            System.out.print("Elija una opcion: ");
            choice = sc.nextInt();

            switch (choice) {
                case 1:
                    System.out.print("Ingrese el numero decimal: ");
                    number = sc.next();
                    System.out.println("El resultado en binario es: " +  ConversorNumerico.decimalToBinary(number));
                    break;
                case 2:
                    System.out.print("Ingrese el numero decimal: ");
                    number = sc.next();
                    System.out.println("El resultado en octal es: " + ConversorNumerico.decimalToOctal(number));
                    break;
                case 3:
                    System.out.print("Ingrese el numero decimal: ");
                    number = sc.next();
                    System.out.println("El resultado en hexadecimal es: " + ConversorNumerico.decimalToHexadecimal(number));
                    break;
                case 4:
                    System.out.print("Ingrese el numero binario: ");
                    number = sc.next();
                    System.out.println("El resultado en decimal es: " + ConversorNumerico.binaryToDecimal(number));
                    break;
                case 5:
                    System.out.print("Ingrese el numero octal: ");
                    number = sc.next();
                    System.out.println("El resultado en decimal es: " + ConversorNumerico.octalToDecimal(number));
                    break;
                case 6:
                    System.out.print("Ingrese el numero hexadecimal: ");
                    number = sc.next();
                    System.out.println("El resultado en decimal es: " + ConversorNumerico.hexadecimalToDecimal(number));
                    break;
                case 7:
                    exit = true;
                    break;
                default:
                    System.out.println("Opcion invalida, intente de nuevo.");
                    break;
            }

        System.out.println("¡Espero y hayas quedado satisfecho!");
}
    }

}
