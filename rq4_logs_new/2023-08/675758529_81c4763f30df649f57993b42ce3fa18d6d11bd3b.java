package lesson4;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {

    }

    public static int[] array() {
        System.out.println("Введите целое положительно число");
        Scanner sc = new Scanner(System.in);
        int t;

        while (true) {
            try {
                t = sc.nextInt();
                if (t < 0) {
                    System.out.println("Введите целое положительное число");
                    sc.nextLine();
                } else {
                    break;
                }
            } catch (Exception e) {
                System.out.println("Введите целое положительное число число");
                sc.nextLine();
            }

        }
        int[] x = new int[t];
        for (int i = 0; i < x.length; i++) {
            x[i] = (int) (Math.random() * 100);

        }
        return x;
    }
}
