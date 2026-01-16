package Module4_KorotaevaSA.Middle;

import java.util.Scanner;

public class Task3 {
    /*
    Заданы длины сторон треугольника. Необходимо определить, может ли существовать треугольник с такими сторонами.
    Сумма любых двух сторон треугольника не может быть меньше третьей стороны
     */
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        System.out.println("Введите длину стороны треугольника x:");
        int x = scan.nextInt();
        System.out.println("Введите длину стороны треугольника y:");
        int y = scan.nextInt();
        System.out.println("Введите длину стороны треугольника z:");
        int z = scan.nextInt();
        if (x > ( y + z ) || y > ( x + z ) || z > ( y + x )) {
            System.out.println("Такой треугольник не существует");
        } else {
            System.out.println("Такой треугольник возможен");
        }
        /*
        Введите длину стороны треугольника x:
        0
        Введите длину стороны треугольника y:
        20
        Введите длину стороны треугольника z:
        10
        Такой треугольник не существует
         */
    }

}