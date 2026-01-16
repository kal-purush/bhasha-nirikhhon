package lesson5.hw1;

import java.util.Random;
import java.util.Scanner;

/**
 Задача 1:
 1.1 Создать двумерный массив, заполнить его случайными числами.
 1.2 Добавить к каждому значению число, которое пользователь будет вводить с консоли.
 1.3 Найти сумму всех получившихся элементов и вывести в консоль
 */
public class Hw1 {
    public static void main(String[] args) {

        int [][] mass = new int[5][5];
        int count = 0;

        Random r = new Random();

        System.out.println("Массив ");
        for (int i = 0; i < mass.length; i++){
            for(int j = 0; j < mass[i].length; j++){
             mass[i][j] = r.nextInt(100);
                System.out.print(mass[i][j] + " ");
            }
            System.out.println();
        }
        System.out.println("Введите целое положительно число");
        Scanner sc = new Scanner(System.in);
        int t;

        while (true) {
            try {
                t = sc.nextInt();
                if (t < 0) {
                    System.out.println("Введите целое положительное число");
                    sc.nextLine();
                } else {
                    break;
                }
            } catch (Exception e) {
                System.out.println("Введите целое положительное число число");
                sc.nextLine();
            }

        }

        System.out.println("Значения массив + пользовательское число ");

        for (int i = 0; i < mass.length; i++){
            for(int j = 0; j < mass[i].length; j++){
                mass[i][j] = mass[i][j] + t;
                System.out.print(mass[i][j] + " ");
                count += mass[i][j];
            }
            System.out.println();
        }
        System.out.println("Cумма всех элементов массива равна " + count);

    }
}