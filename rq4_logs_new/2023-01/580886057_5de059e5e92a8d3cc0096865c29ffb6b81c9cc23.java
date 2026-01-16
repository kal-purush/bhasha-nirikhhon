package pro.sky.java.course1.lesson8;

import java.util.Arrays;
import java.util.Random;

public class Program {
    public static void main(String[] args) {

        int[] arr = generateRandomArray();

        // Вывод всех элементов массива в одну строку (чтобы проверить решения задач)
        System.out.println(Arrays.toString(arr));


        ////////// Task 1 //////////
        System.out.println("\nTask 1:");

        int totalSum = 0;
        for (int sum : arr) {
            totalSum += sum;
        }
        System.out.println("Сумма трат за месяц составила " + totalSum + " рублей");


        ////////// Task 2 //////////
        System.out.println("\nTask 2:");

        int min = arr[0];
        int max = arr[0];
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] < min)
                min = arr[i];
            if (arr[i] > max)
                max = arr[i];
        }
        System.out.println("Минимальная сумма трат за день составила " + min + " рублей. Максимальная сумма трат за день составила " + max + " рублей");


        ////////// Task 3 //////////
        System.out.println("\nTask 3:");

        double averageSum = (double) totalSum / arr.length;
        System.out.printf("Средняя сумма трат за месяц составила %.2f рублей", averageSum);


        ////////// Task 4 //////////
        System.out.println("\n\nTask 4:");

        char[] reverseFullName = {'n', 'a', 'v', 'I', ' ', 'v', 'o', 'n', 'a', 'v', 'I'};
        for (int i = reverseFullName.length - 1; i >= 0; i--) {
            System.out.print(reverseFullName[i]);
        }
    }

    // Метод, генерирующий массив из 30 случайных чисел в интервале от 100_000 до 200_000
    public static int[] generateRandomArray() {
        Random random = new Random();
        int[] arr = new int[30];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = random.nextInt(100_000) + 100_000;
        }
        return arr;
    }
}