package lesson5.hw3;

import java.util.Scanner;

/**
 * Задача *:
 * Даны числа n и m. Создайте массив A[n][m] и заполните его змейкой (см. пример).
 * Формат входных данных:
 * Программа получает на вход два числа n и m.
 * Формат выходных данных:
 * Программа должна вывести полученный массив, отводя на вывод каждого числа ровно 3
 * символа
 */
public class Hw3 {
    public static void main(String[] args) {

        System.out.println("Введите два целых положительных числа через Enter ");
        Scanner sc = new Scanner(System.in);
        int t;
        int c;

        while (true) {
            try {
                c = sc.nextInt();
                t = sc.nextInt();
                if (t < 0 && c < 0) {
                    System.out.println("Введите два положительных числа ");
                    sc.nextLine();
                } else {
                    break;
                }
            } catch (Exception e) {
                System.out.println("Введите два целых положительных числа ");
                sc.nextLine();
            }

        }

        int[][] mass = new int[c][t];
        int count = 0;

        for (int i = 0; i < mass.length; i++) {
            for (int j = 0; j < mass[i].length; j++) {
                mass[i][j] = count;
                count++;
            }
        }

        for (int i = 0; i < mass.length; i++) {
            if (i % 2 == 0) {
                for (int j = 0; j < mass[i].length; j++) {
                    String format = String.format("%3d", mass[i][j]);
                    System.out.print(format + " ");
                }

            } else {

                for (int j = mass[i].length - 1; j >= 0; j--) {
                    String format = String.format("%3d", mass[i][j]);
                    System.out.print(format + " ");
                }
            }
            System.out.println();
        }

    }
}

