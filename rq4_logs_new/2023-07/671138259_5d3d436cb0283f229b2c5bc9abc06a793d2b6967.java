import java.sql.SQLOutput;

public class Main {
    public static void main(String[] args) {
        for (int i = 1; i <= 10; i++) { // Задача 1
            System.out.println("i = " + i);
        }
        for (int i = 10; i > -11; i = i - 1) { // Задача 4
            System.out.println("i = " + i);
        }
        for (int i = 2; i <= 17; i = i + 2) { // Задача 3
            System.out.println("i = " + i);
        }
        for (int i = 10; i > 0; i = i - 1) {
            System.out.println("i = " + i); // Задача 2
        }
        for (int i = 1904; i <= 2096; i = i + 4) { // Задача 5
            System.out.println(i + " год является високосным");
        }
        for (int i = 7; i <= 98; i+= 7) { // Задача 6
            System.out.println(i);
        }
        for (int i =1; i<513; i*=2) { // Задача 7
        System.out.println(i);
        }
        int salary = 65535;
        int total =0;
        for (int i=0; i<12; i++) {
            total = total + salary;
            System.out.println(total);
        }
    }
}