package Module4_KorotaevaSA.Hard;

import java.util.Scanner;

public class Task2 {
    public static void main(String[] args) {
        /*
        2. Заданы координаты двух фигур — пешки и слона. Определить, находится ли пешка под боем слона. Слон ходит
        по диагонали, а пешка находится под боем, если стоит на одной диагонали со слоном. Шахматное поле состоит из
        восьми клеток в ширину и в длину.
         */

        Scanner scan = new Scanner(System.in);
        System.out.println("Шахматное поле 8*8. (a-h) на (1-8) клеток.");
        System.out.println("Введите координату пешки в формате БукваЦифра:");
        String pawnXY = scan.next();
        String pawnX = pawnXY.substring(0, 1);
        String pawnY = pawnXY.substring(1,2);
        char pawnXc = pawnX.charAt(0);
        int pawnYi = Integer.parseInt(pawnY);

        System.out.println("Введите координату слона в формате БукваЦифра:");
        String bishopXY = scan.next();
        String bishopX = bishopXY.substring(0, 1);
        String bishopY = bishopXY.substring(1,2);
        char bishopXc = bishopX.charAt(0);
        int bishopYi = Integer.parseInt(bishopY);

        int deltaX = Math.abs(pawnXc - bishopXc);
        int deltaY = Math.abs(pawnYi - bishopYi);

        if (deltaX == deltaY) {
            System.out.println("Пешка под боем");
        } else {
            System.out.println("Угрозы для пешки нет");
        }
    }
}