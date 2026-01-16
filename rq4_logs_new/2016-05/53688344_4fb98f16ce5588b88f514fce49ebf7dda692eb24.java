/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pkg01.calculo;

import java.util.Scanner;

/**
 *
 * @author JULAY
 */
public class exerc01 {

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        
       
        System.out.println("Introduza o valor numer 1:");
        double x = input.nextDouble();
        System.out.println("Introduza o valor numer 2:");
         double y = input.nextDouble();

        double m = (x + y) / 2;
        System.out.println("Media = " + m);
    }
}