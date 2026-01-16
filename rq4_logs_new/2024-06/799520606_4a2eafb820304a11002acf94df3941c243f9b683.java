package Module4_KorotaevaSA.Middle;

import java.util.Scanner;

public class Task5 {
    public static void main(String[] args) {
        /*
        5. Написать программу, которая будет определять, есть ли действительные корни у квадратного уравнение.
        Квадратное уравнение можно записать следующим образом: ax^2+bx+c=0. Пользователь вводит параметры a, b и c.
        Если дискриминант квадратного уравнения больше или равен 0, то уравнение имеет действительные корни, если
        дискриминант меньше нуля, то уравнение не имеет корней.
        D = b^2 - 4ac
         */
        Scanner scan = new Scanner(System.in);
        System.out.println("Задайте число a:");
        double a = scan.nextDouble();
        System.out.println("Задайте число b:");
        double b = scan.nextDouble();
        System.out.println("Задайте число c:");
        double c = scan.nextDouble();
        double D = Math.pow(b,2) - 4*a*c;
        if (D < 0) {
            System.out.println("Нет действительных корней");
        } else {
            System.out.println("Есть действительные корни. Дискриминант D = " + D);
        }
    }
}