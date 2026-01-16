import java.util.Random;
import java.util.Scanner;

/*Найти среднее арифметическое элементов массива.*/
public class Average {
    public static void main(String[] args) {

        System.out.print("Задайте количество элементов массива ");
        Scanner scanner = new Scanner(System.in);
        int len = scanner.nextInt();
        System.out.print("Задайте нижнюю границу для интервала элементов массива ");
        int min = scanner.nextInt();
        System.out.print("Задайте верхнюю границу для интервала элементов массива ");
        int max = scanner.nextInt();
        int[] array = new int[len];
        int summa = 0;

        Random random = new Random();
        for (int i = 0; i < array.length; i++) {
            array[i] = (int) (Math.random() * (max - min));
            System.out.print(" " + array[i] + " ");
        }
        System.out.println(" ");

        for (int i = 0; i < array.length; i++) {
            summa = summa + array[i];
        }
        double average = summa / array.length;
        System.out.printf("Среднее арифметическое элементов массива данного массива равно %.2f ", average);
    }
}
