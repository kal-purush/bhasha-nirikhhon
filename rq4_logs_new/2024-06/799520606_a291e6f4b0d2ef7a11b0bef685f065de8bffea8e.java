package Module4_KorotaevaSA.Easy;

import java.util.Scanner;

public class Task2 {
    public static void main(String[] args) {
        /*
        Вводится значение от 1 до 4. Вывести на консоль: Зима, если введено 1, Весна — 2, Лето — 3, Осень — 4
         */
            Scanner scanner = new Scanner(System.in);
            System.out.println("Введите цифру от 1 до 4:");
            int season = scanner.nextInt();

            switch (season) {
                case (1):
                {
                    System.out.println("Зима");
                    break;
                }
                case (2):
                {
                    System.out.println("Весна");
                    break;
                }
                case (3):
                {
                    System.out.println("Лето");
                    break;
                }
                case (4):
                {
                    System.out.println("Осень");
                    break;
                }
                default: {
                    System.out.println("Не корректный ввод");
                }
            }
    }
}
