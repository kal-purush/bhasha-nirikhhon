package main.ua.goit;

import java.util.Scanner;

public class InputHelper {

    static Scanner scanner;
    static String inputNumberText = "Введите число (позитивное в одной из систем 3-16):";
    static String inputSystemText = "Введите систему вашего числа (3-16):";

    static {
        scanner = new Scanner(System.in);
    }

    // Enter number
    public static String enterNumber() {
        System.out.println(inputNumberText);
        String enteredValue = scanner.nextLine();
        while (!isNumberValid(enteredValue)) {
            System.out.println("Невалидное число.\n" + inputNumberText);
            enteredValue = scanner.nextLine();
        }

        return enteredValue;
    }

    // Enter system
    public static int enterSystem() {
        System.out.println(inputSystemText);
        String enteredValue = scanner.nextLine();
        while (!isSystemNumberValid(enteredValue)) {
            System.out.println("Невалидное значение системы.\n" + inputSystemText);
            enteredValue = scanner.nextLine();
        }

        return Integer.parseInt(enteredValue);
    }

    // Check number
    private static boolean isNumberValid(String enteredValue) {
        boolean result = true;
        try {
            String acceptableChars = "0123456789abcdef";
            for (char enteredChar : enteredValue.toCharArray()) {
                if (!acceptableChars.contains(String.valueOf(enteredChar).toLowerCase())) result = false;
            }
        } catch (NumberFormatException nfe) {
            result = false;
        }
        
        return result;
    }

    // Check system
    public static boolean isSystemNumberValid(String enteredValue) {
        boolean result = false;
        try {
            Integer enteredSystem = Integer.parseInt(enteredValue);
            if (enteredSystem > 2 && enteredSystem < 17) result = true;
        } catch (NumberFormatException nfe) {
            result = false;
        }

        return result;
    }
}