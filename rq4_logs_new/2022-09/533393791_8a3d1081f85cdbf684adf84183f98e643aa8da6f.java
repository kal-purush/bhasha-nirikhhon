package lesson2;

import java.util.Scanner;

public class Task2 {
    public static void main ( String[] args ) {

        Task2 triangle = new Task2 ( );
        triangle.calculate ( );
    }

    public void calculate () {
        Scanner scanner = new Scanner ( System.in );

        System.out.println ("Enter A x1:" );
        double x1 = scanner.nextDouble ();

        System.out.println ("Enter A y1:" );
        double y1 = scanner.nextDouble ( );

        System.out.println ("Enter B x2:" );
        double x2 = scanner.nextDouble ();

        System.out.println ("Enter B y2:" );
        double y2 = scanner.nextDouble ( );

        System.out.println ("Enter C x3:" );
        double x3 = scanner.nextDouble ( );

        System.out.println ("Enter C y3:" );
        double y3 = scanner.nextDouble ( );

        double square = (x1 * (y2 - y3) + x2 * ( y3 - y1 ) + x3 * ( y1 - y2 )) / 2;

        System.out.println ("Square is: " + square );
    }
}