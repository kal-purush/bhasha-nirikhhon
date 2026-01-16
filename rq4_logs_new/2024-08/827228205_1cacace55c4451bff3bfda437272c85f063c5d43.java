package ru.otus.java.basic.safarov.homework01;

import java.util.Scanner;

public class MainApplication {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Укажите номер для выбора метода:\n" +
                    "0 - выход\n" +
                    "1 - greetings()\n" +
                    "2 - checkSign(..)\n" +
                    "3 - selectColor()\n" +
                    "4 - compareNumbers()\n" +
                    "5 - addOrSubtractAndPrint(..)");

            int choice = scanner.nextInt();
            if (choice == 1) {
                greetings();
            } else if (choice == 2) {
                System.out.println("Укажите 3 целых числа для проверки суммы на равенстов нулю.");
                System.out.print("a: ");
                int a = scanner.nextInt();
                System.out.print("b: ");
                int b = scanner.nextInt();
                System.out.print("c: ");
                int c = scanner.nextInt();
                checkSign(a, b, c);
            } else if (choice == 3) {
                selectColor();
            } else if (choice == 4) {
                compareNumbers();
            } else if (choice == 5) {
                System.out.println("Укажите параметры для метода addOrSubtractAndPrint()");
                System.out.print("initValue (целое число): ");
                int initValue = scanner.nextInt();
                System.out.print("delta (целое число): ");
                int delta = scanner.nextInt();
                System.out.print("increment (boolean): ");
                boolean increment = scanner.nextBoolean();
                addOrSubtractAndPrint(initValue, delta, increment);
            } else if (choice == 0) {
                break;
            } else {
                System.out.println("Ваш выбор не соответствует предложенным вариантам.\n" +
                        "Повторите выбор варианта.");
            }
            System.out.println("\n========================\n");
        }

    }

    public static void greetings() {
        System.out.println("Hello\nWorld\nfrom\nJava");
    }

    public static void checkSign(int a, int b, int c) {
        int result = a + b + c;
        if (result >= 0) {
            System.out.println("Сумма положительная");
        } else {
            System.out.println("Сумма отрицательная");
        }
    }

    public static void selectColor() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Введите целое число: ");
        int data = scanner.nextInt();
        if (data <= 10) {
            System.out.println("Красный");
        } else if (data <= 20) {
            System.out.println("Желтый");
        } else {
            System.out.println("Зеленый");
        }
    }

    public static void compareNumbers() {
        int a = (int) (Math.random() * 10) * (((int) (Math.random() * 2) + 1 == 1 ? -1 : 1));
        int b = (int) (Math.random() * 10) * (((int) (Math.random() * 2) + 1 == 1 ? -1 : 1));
        if (a >= b) {
            System.out.println("a >= b");
        } else {
            System.out.println("a < b");
        }
    }

    public static void addOrSubtractAndPrint(int initValue, int delta, boolean increment) {
        int result = initValue;
        if (increment) {
            result += delta;
        } else {
            result -= delta;
        }
        System.out.println(result);
    }
}