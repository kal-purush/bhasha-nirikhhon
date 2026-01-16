package week2;

import java.util.Scanner;

/**
 * 5.46
 * @author Sydney
 */
public class C5N46 {

    public static void main(String[] args) {

        {
            String original, reverse = "";
            Scanner in = new Scanner(System.in);

            System.out.println("Enter a string: ");
            original = in.nextLine();

            int length = original.length();

            for (int i = length - 1; i >= 0; i--) {
                reverse = reverse + original.charAt(i);
            }

            System.out.println("Reverse of entered string is: " + reverse);
        }

    }
}