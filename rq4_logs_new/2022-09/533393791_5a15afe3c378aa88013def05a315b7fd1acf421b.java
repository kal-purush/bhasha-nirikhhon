package ua.lyashko.lesson7;

public class Main {
    public static void main ( String[] args ) {
        Vector[] vector = Vector.create ( 5 );
        System.out.println ( ( ( vector[0] + " , " + vector[1] ) ) );
        System.out.println ("Vector length: " + vector[0].vectorLenghth () );
        System.out.println ("Vector product: " + vector[0].vectorProduct ( vector[1] ) );
        System.out.println ("Vector cos: " + vector[0].cos ( vector[1] ) );
        System.out.println ("Vector add: " + vector[0].add ( vector[1] ) );
        System.out.println ("Vector substract: " + vector[0].substract ( vector[1] ) );
    }
}