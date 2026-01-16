/*Напишите консольную программу,
в которой пользователь вводит с клавиатуры число.
Если число одновременно меньше 9 и больше 2,
то программа выводит "Число больше 5 и меньше 10".
Иначе программа выводит сообщение "Неизвестное число".*/

import java.util.Scanner;

public class Ch2 {

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.println("Input numm");
        int num1 = in.nextInt();

        if (num1 <= 9 && num1 >= 2) {

            System.out.println("Число больше 5 и меньше 10");

        } else {
            System.out.println("neiz chislo");

        }
    }
}