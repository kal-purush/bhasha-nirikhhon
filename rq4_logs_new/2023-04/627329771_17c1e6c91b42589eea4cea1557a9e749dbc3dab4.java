package org.flowershop.utils.Scan;

import java.util.InputMismatchException;
import java.util.Scanner;

public class Scan {
    public static int askForInt(String message) {

        Scanner sc = new Scanner(System.in);
        int integer  = 0;
        boolean ok = false;
        do {
            System.out.println(message);
            try {
                integer = sc.nextInt();
                ok = true;
            } catch (InputMismatchException ex) {
                System.out.println("ERROR. Input type mismatch\n");
            }
            //sc.nextLine();
        } while (!ok);
        return integer;
    }

    public static String askForString(String message) {

        Scanner sc = new Scanner(System.in);
        String cadena  = "";
        boolean ok = false;
        do {
            System.out.println(message);
            try {
                cadena = sc.nextLine();
                ok = true;
            } catch (InputMismatchException ex) {
                System.out.println("ERROR. Input type mismatch\n");
            }
            //sc.nextLine();
        } while (!ok);
        return cadena;
    }
}