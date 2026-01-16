package seminar1;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
//        System.out.println("Hello, world!");
        inputNameAndPrintHi();
        maxArr();
    }

    public static void inputNameAndPrintHi() {
        Scanner in = new Scanner(System.in);
        System.out.println("Введите имя: ");
        String name = in.nextLine();
        System.out.println("Привет, " + name + "!");
        in.close();
    }

    public static void maxArr() {
        int[] arr = {1, 1, 1, 0, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1};
        int count = 0;
        int temp = 0;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == 1) {
                temp++;
                if (count < temp) {
                    count = temp;
                }
            }
            else {
                temp = 0;
            }
        }
        System.out.print(count);
    }
}