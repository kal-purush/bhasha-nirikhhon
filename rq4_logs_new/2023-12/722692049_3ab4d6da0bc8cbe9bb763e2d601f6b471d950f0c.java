package main.java.com.magicvet;
import java.util.Scanner;
public class Main {
    static String PASSWORD = "default";
    static Scanner SCANNER = new Scanner(System.in);
    public static void main(String[] args) {
        run();
    }
    static void run() {
        boolean accepted = false;
        for (int i = 0; i < 3; i++) {
            System.out.println("Password: ");
            String input = SCANNER.nextLine();
            if (PASSWORD.equals(input)) {
                accepted = true;
                break;
            } else {
                System.out.println("Access denied. Please check your password.");
            }
        }
        System.out.println(accepted ? "Welcome to the Magic Vet!" : "Application has been blocked.");
    }
}

