package com.company;

import java.util.*;

public class Main {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter [2+2] number or two Rome number  from I to X:[V+V] + Enter ");

        int number1, number2;
        char operation = 0;
        int result;
        String userInput = scanner.nextLine();


        char[] under_char = new char[10]; // пустой символьный массив

        for (int i = 0; i < userInput.length(); i++) { // Заполнить  массив символами строки которую вводит пользователь и по ходу  знак словить операции
            under_char[i] = userInput.charAt(i);
            if (under_char[i] == '+') {
                operation = '+';
            }
            if (under_char[i] == '-') {
                operation = '-';
            }
            if (under_char[i] == '*') {
                operation = '*';
            }
            if (under_char[i] == '/') {
                operation = '/';
            }
        }
        Character firstSymbolInput = userInput.charAt(0);//проверка символа
        if (Character.isLetter(firstSymbolInput)) {
            String under_charString = String.valueOf(under_char);
            String[] blocks = under_charString.split("[+-/*]");
            String string1 = blocks[0];
            String string2 = blocks[1];
            String string3 = string2.trim();
            number1 = romanToNumber(string1);
            number2 = romanToNumber(string3);
            result = calculated(number1, number2, operation);
            System.out.println("Result for Roman numerals");
            String resultRoman = convertNumToRoman(result);
            System.out.println(string1 + " " + operation + " " + string3 + " = " + resultRoman);
        } else {
            try {
                String under_charString = String.valueOf(under_char);
                String[] blocks = under_charString.split("[+-/*]");
                String string1 = blocks[0];
                String string2 = blocks[1];
                String string3 = string2.trim();
                number1 = Integer.parseInt(string1);
                number2 = Integer.parseInt(string3);
                if ((number1 > 10 || number1 < 0) || (number2 > 10 || number2 < 0)) {
                    throw new IllegalArgumentException();
                }
                result = calculated(number1, number2, operation);
                System.out.println("Result for Arabic numerals");
                System.out.println(number1 + " " + operation + " " + number2 + " = " + result);
            } catch (NumberFormatException e) {
                System.out.println("Error Number");
            } catch (IllegalArgumentException e) {
                System.out.println("Error 1-10");
            } catch (Exception e) {
                System.out.println("Error");
            }
        }
    }


    private static String convertNumToRoman(int numArabian) { //конвертация римских чисел
        String[] roman = {"O", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII", "XVIII", "XIX", "XX",
                "XXI", "XXII", "XXIII", "XXIV", "XXV", "XXVI", "XXVII", "XXVIII", "XXIX", "XXX", "XXXI", "XXXII", "XXXIII", "XXXIV", "XXXV", "XXXVI", "XXXVII", "XXXVIII", "XXXIX", "XL",
                "XLI", "XLII", "XLIII", "XLIV", "XLV", "XLVI", "XLVII", "XLVIII", "XLIX", "L", "LI", "LII", "LIII", "LIV", "LV", "LVI", "LVII", "LVIII", "LIX", "LX",
                "LXI", "LXII", "LXIII", "LXIV", "LXV", "LXVI", "LXVII", "LXVIII", "LXIX", "LXX",
                "LXXI", "LXXII", "LXXIII", "LXXIV", "LXXV", "LXXVI", "LXXVII", "LXXVIII", "LXXIX", "LXXX",
                "LXXXI", "LXXXII", "LXXXIII", "LXXXIV", "LXXXV", "LXXXVI", "LXXXVII", "LXXXVIII", "LXXXIX", "XC",
                "XCI", "XCII", "XCIII", "XCIV", "XCV", "XCVI", "XCVII", "XCVIII", "XCIX", "C"
        };
        final String s = roman[numArabian];
        return s;
    }

    private static int romanToNumber(String roman) { //проверка римских чисел
        try {
            if (roman.equals("I")) {
                return 1;
            } else if (roman.equals("II")) {
                return 2;
            } else if (roman.equals("III")) {
                return 3;
            } else if (roman.equals("IV")) {
                return 4;
            } else if (roman.equals("V")) {
                return 5;
            } else if (roman.equals("VI")) {
                return 6;
            } else if (roman.equals("VII")) {
                return 7;
            } else if (roman.equals("VIII")) {
                return 8;
            } else if (roman.equals("IX")) {
                return 9;
            } else if (roman.equals("X")) {
                return 10;
            }
        } catch (InputMismatchException e) {
            throw new InputMismatchException("Invalid data format");
        }
        return -1;
    }

    public static int calculated(int num1, int num2, char op) { //калькулятор
        int result = 0;
        switch (op) {
            case '+':
                result = num1 + num2;
                break;
            case '-':
                result = num1 - num2;
                break;
            case '*':
                result = num1 * num2;
                break;
            case '/':
                try {
                    result = num1 / num2;
                } catch (ArithmeticException | InputMismatchException e) {
                    System.out.println("Exception : " + e);
                    System.out.println("Only integer non-zero parameters allowed");

                    break;
                }
                break;
            default:
                throw new IllegalArgumentException("Incorrect operation sign");
        }
        return result;
    }
}















