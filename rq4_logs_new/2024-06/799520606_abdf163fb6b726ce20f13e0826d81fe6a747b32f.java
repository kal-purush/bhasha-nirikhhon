package Module5_KorotaevaSA.Easy;

import java.util.Scanner;

public class Task1 {
    public static void main(String[] args) {
        /*
        Допишите программу, которая возводит число в целочисленную степень. Число и степень вводятся с клавиатуры.
        Использовать цикл для возведения в степень.
         */
        System.out.println("Введите число и степень");
        Scanner scanner = new Scanner(System.in);
        int a = scanner.nextInt();
        int n = scanner.nextInt();
        System.out.println("Результат: ");
        int i = 1;
        int res = 1;
        do {
            res = res * a;
            i++;
            System.out.println(res);
        } while (i <= n);
    }
}