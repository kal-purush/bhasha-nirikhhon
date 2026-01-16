package ua.lyashko.lesson3;

import java.util.Scanner;

public class Task2 {
    Scanner scanner = new Scanner ( System.in );
    String line = scanner.nextLine ( );

    public static void main ( String[] args ) {
        Task2 str = new Task2 ( );
        str.deleteSpace ( );
        str.detector ();
    }

    public void deleteSpace () {
        line = line.replaceAll ( "\\s" , "" );
        System.out.println ( line );
    }

    public void detector ( ) {
        StringBuilder buffer = new StringBuilder ( line );
        buffer.reverse();
        String data = buffer.toString();
        if(line.equals(data)){
            System.out.println("Given String is palindrome " + line);
        }else{
            System.out.println("Given String is not palindrome " + line);
        }
    }
}