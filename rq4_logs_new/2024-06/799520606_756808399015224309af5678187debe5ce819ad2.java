package Module4_KorotaevaSA.Hard;

import java.util.Scanner;

public class Task1 {
    public static void main(String[] args) {
        /*
        1. Написать программу, которая в зависимости от выбора пользователя вычисляет площадь одной из трёх
        геометрических фигур: прямоугольника, треугольника или круга.
         */
        Scanner scan = new Scanner(System.in);
        System.out.println("Укажите номер фигуры для вычисления площади (1 - прямоугольник, 2 - треугольник " +
                "3 - круг):");
        int x = scan.nextInt();
        switch (x) {
            case (1): {
                //1 - прямоугольник S = a * b
                System.out.println("Задайте длину стороны a:");
                double a = scan.nextDouble();
                System.out.println("Задайте длину стороны b:");
                double b = scan.nextDouble();
                double S = a * b;
                System.out.println("Площадь прямоугольника S = " + S);
                break;
            }
            case (2): {
                //2 - треугольник
                // площадь через полупериметр S = корень кв из (p(p-a)(p-b)(p-c))
                System.out.println("Задайте длину стороны a:");
                double a = scan.nextDouble();
                System.out.println("Задайте длину стороны b:");
                double b = scan.nextDouble();
                System.out.println("Задайте длину стороны c:");
                double c = scan.nextDouble();

                double p = (a + b + c) / 2;
                double St = Math.sqrt(p * ( p - a ) * ( p - b ) * ( p - c));
                System.out.println("Площадь треугольника равна = " + St);
                break;
            }
            case (3): {
                //3 - круг S = Пr^2
                System.out.println("Задайте радиус круга:");
                double r = scan.nextDouble();
                double Sc = Math.PI * Math.pow(r, 2);
                System.out.println("Площадь круга равна = " + Sc);
                break;
            }
            default: {
                System.out.println("Не корректный ввод");
            }
        }
    }
}