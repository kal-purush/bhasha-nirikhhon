package pro.sky.java.course1.additional.tasks3;

public class Program {
    public static void main(String[] args) {

        /////////// Task 4 //////////
        System.out.println("Task 4");

        for (int i = 1; i <= 30; i++) {
            System.out.print(i + ":");
            if (i % 3 == 0) {
                System.out.print(" ping");
            }
            if (i % 5 == 0) {
                System.out.print(" pong");
            }
            System.out.println();
        }


        /////////// Task 5 //////////
        System.out.println("\nTask 5:");

        int a = 0;
        int b = 1;
        int c;
        System.out.print(a + " " + b + " ");
        for (int i = 0; i < 8; i++) {
            c = b + a;
            a = b;
            b = c;
            System.out.print(b + " ");
        }
    }
}