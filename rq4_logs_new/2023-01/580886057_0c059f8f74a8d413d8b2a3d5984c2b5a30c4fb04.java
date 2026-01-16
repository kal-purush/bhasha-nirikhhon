package pro.sky.java.course1.additional.tasks4;

import java.util.Arrays;

public class Program {
    public static void main(String[] args) {

        /////////// Task 5 //////////
        System.out.println("Task 5:");

        int[][] matrix = new int[3][3];
        int x = 1;

        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                if (i % 2 == 0 && j % 2 == 0) {
                    matrix[i][j] = x;
                } else if (i % 2 != 0 && j % 2 !=0) {
                    matrix[i][j] = x;
                } else {
                    matrix[i][j] = 0;
                }
            }
        }

        for (int[] row : matrix) {
            for (int column : row) {
                System.out.print(column + " ");
            }
            System.out.println();
        }


        /////////// Task 6 //////////
        System.out.println("\nTask 6:");

        int[] arr1 = {5, 4, 3, 2, 1};
        System.out.print("Массив до преобразования: ");
        System.out.println(Arrays.toString(arr1));
        int[] arrReverse = new int[arr1.length];
        for (int i = arr1.length - 1, j = 0; i >= 0; i--) {
            arrReverse[j] = arr1[i];
            j++;
        }
        arr1 = Arrays.copyOf(arrReverse, arrReverse.length);
        System.out.print("Массив после преобразования: ");
        System.out.println(Arrays.toString(arr1));


        /////////// Task 7 //////////
        System.out.println("\nTask 7:");

        int[] arr2 = {5, 4, 3, 2, 1};
        System.out.print("Массив до преобразования: ");
        System.out.println(Arrays.toString(arr2));
        int k = 0;  // Переменная, которая будет использоваться при обмене элементов массива
        for (int i = 0; i < arr2.length/2; i++) {
            k = arr2[arr2.length - i - 1];
            arr2[arr2.length - i - 1] = arr2[i];
            arr2[i] = k;
        }
        System.out.print("Массив после преобразования: ");
        System.out.println(Arrays.toString(arr2));


        /////////// Task 8* //////////
        System.out.println("\nTask 8*:");

        int[] arr3 = {-6, 2, 5, -8, 8, 10, 4, -7, 12, 1};
        int result1 = -2;
        int firstNum = 0;
        int secondNum = 0;

        for (int i = 0; i < arr3.length; i++) {
            for (int j = i + 1; j < arr3.length; j++) {
                if (arr3[i] + arr3[j] == result1) {
                    firstNum = arr3[i];
                    secondNum = arr3[j];
                }
            }
        }

        if (firstNum != 0 && secondNum !=0) {
            System.out.println("Сумма чисел " + firstNum + " и " + secondNum + " равна " + result1);
        } else {
            System.out.println("В массиве отсутсвуют значения чисел, сумма которых равна " + result1);
        }


        /////////// Task 9* //////////
        System.out.println("\nTask 9*:");

        int[] arr4 = {-6, 2, 5, -8, 8, 10, 4, -7, 12, 1};
        int result2 = -2;

        for (int i = 0; i < arr4.length; i++) {
            for (int j = i + 1; j < arr4.length; j++) {
                if (arr4[i] + arr4[j] == result2) {
                    System.out.println("Сумма чисел " + arr4[i] + " и " + arr4[j] + " равна " + result2);
                }
            }
        }
    }
}