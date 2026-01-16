
import java.util.Scanner;

public class ASCII {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter a character: ");
        char character = sc.next().charAt(0);
        sc.close();
        int asciiValue = character;
        System.out.println("ASCII Value of '" + character + "' is: " + asciiValue);
    }
}
    