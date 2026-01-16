package Module4_KorotaevaSA.Middle;

import java.util.Scanner;

public class Task1 {
    public static void main(String[] args) {
/*
        1. Определить, какое из трёх введённых пользователем значений наименьшее, наибольшее, среднее.

 */
        final int RANGE_3 = 3;
        final int RANGE_0 = 0;

        Scanner scanner = new Scanner(System.in);
        System.out.println("Введите три числа: ");
        int[] range = new int[RANGE_3];
        //запись вводимых значений с консоли в массив
        for ( int i = RANGE_0; i < RANGE_3; i++ ) {
            range[i] = scanner.nextInt();
        }
        //поиск минимального значения в полученном массиве
        int min = range[RANGE_0];
        for ( int i = RANGE_0; i < range.length; i++ ) {
            min = Math.min(min, range[i]);
        }
        //поиск максимального значения в полученном массиве
        int max = range[RANGE_0];
        for ( int i = RANGE_0; i < range.length; i++ ) {
            max = Math.max(max, range[i]);
        }
        //поиск среднего значения
        int mid = 0;
        int x;   //промежуточная переменная
        for ( int i = RANGE_0; i < range.length; i++ ) {
            x = range[i];
            if (x > min & x < max) {
                mid = x;
        }
    }
        System.out.println("Min = " + min);
        System.out.println("Max = " + max);
        if ( mid == RANGE_0) {
            System.out.println("Mid не удалось определить");
        } else {
            System.out.println("Mid = " + mid);
        }
        /* результат:
        Введите три числа:
        33
        23
        13
        Min = 13
        Max = 33
        Mid = 23
         */
    }
}