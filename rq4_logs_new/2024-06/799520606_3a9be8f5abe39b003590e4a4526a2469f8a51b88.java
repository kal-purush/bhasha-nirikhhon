package Module4_KorotaevaSA.Easy;

import java.util.Scanner;

public class Task1 {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Введите число X:");
        int x = scanner.nextInt();
        int y ;
        if (x>0) {
            y = 2 * x;
            System.out.printf("При x = %d, результат y = 2x равен: %d", x, y);
        } else {
            y = -2 * x;
            System.out.printf("При x = %d, результат y = -2x равен: %d", x, y);
        }
    }
}