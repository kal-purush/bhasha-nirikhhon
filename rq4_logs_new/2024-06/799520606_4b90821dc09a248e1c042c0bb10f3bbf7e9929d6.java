package Module4_KorotaevaSA.Middle;

import java.util.Scanner;

public class Task2 {
    /*
    2. Вводятся два числа (большее и меньшее). Определить, кратно ли первое число второму, то есть делится ли первое
    число нацело на второе. Вывести на экран сообщение об этом, а также остаток от деления, если первое число
    не кратно второму.
     */
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        System.out.println("Введите x:");
        int x = scan.nextInt();
        System.out.println("Введите y:");
        int y = scan.nextInt();
        if (x%y == 0) {
            System.out.println("Число x кратно числу y");
        } else {
            int z = x%y;
            System.out.println("Число x не кратно числу y. Остаток от деления x на y равен: " + z);
        }
        /* результат
        Введите x:
        14
        Введите y:
        5
        Число x не кратно числу y. Остаток от деления X на Y равен: 4
         */
    }
}