package org.example.homework7;

import java.util.Scanner;
import java.util.regex.Pattern;

public class EmailValidator {
    public static void main(String[] args) {
        emailValidator();
    }

    public static void emailValidator() {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter an email: ");
        String email = scanner.nextLine();
        String regexPattern = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
                + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$";

        boolean matches = Pattern.matches(regexPattern, email);
        System.out.println("Does email matches pattern? " + matches);
    }
}