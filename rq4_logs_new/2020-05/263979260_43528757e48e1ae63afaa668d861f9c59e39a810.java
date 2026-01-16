package com.company;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {

        System.out.println("1. Задать целочисленный массив, состоящий из элементов 0 и 1. Например: [ 1, 1, 0, 0, 1, 0, 1, 1, 0, 0 ]. С помощью цикла и условия заменить 0 на 1, 1 на 0");
        int[] arr1 = {1, 1, 0, 0, 1, 0, 1, 1, 0, 0};
        System.out.println("Первоночальный массив arr1 " + Arrays.toString(arr1));
        for (int i = 0; i < arr1.length; i++) {
            if (arr1[i] == 0) {
                arr1[i] = 1;
            } else {
                arr1[i] = 0;
            }
        }
        System.out.println("Новый массив arr1 " + Arrays.toString(arr1));

        System.out.println("2. Задать пустой целочисленный массив размером 8. С помощью цикла заполнить его значениями 0 3 6 9 12 15 18 21");
        int[] arr2 = new int[8];
        for (int i = 0; i < arr2.length; i++) {
            arr2[i] = i * 3;
        }
        System.out.println("Массив arr2 " + Arrays.toString(arr2));

        System.out.println("3. Задать массив [ 1, 5, 3, 2, 11, 4, 5, 2, 4, 8, 9, 1 ] пройти по нему циклом, и числа меньшие 6 умножить на 2");
        int[] arr3 = {1, 5, 3, 2, 11, 4, 5, 2, 4, 8, 9, 1};
        System.out.println("Первоночальный массив arr3 " + Arrays.toString(arr3));
        for (int i = 0; i < arr3.length; i++) {
            if (arr3[i] < 6) {
                arr3[i] = arr3[i] * 2;
            }
        }
        System.out.println("Новый массив arr3 " + Arrays.toString(arr3));

        System.out.println("4. Создать квадратный двумерный целочисленный массив (количество строк и столбцов одинаковое), и с помощью цикла(-ов) заполнить его диагональные элементы единицами");
        int n = 5; // размер массива типо n x n
        int[][] arr4 = new int[n][n];

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (i == j) {
                    arr4[i][j] = 1;
                    System.out.print(arr4[i][j] + " ");
                } else {
                    arr4[i][j] = 0;
                    System.out.print(arr4[i][j] + " ");
                }
            }
            System.out.println();
        }

        System.out.println("5. ** Задать одномерный массив и найти в нем минимальный и максимальный элементы (без помощи интернета)");

        int[] arr5 = {1, 1, 0, 0, 8, 0, 1, 1, 0, -1};
        System.out.println("Первоночальный массив arr5 " + Arrays.toString(arr1));
        int max = arr5[0];
        int min = arr5[0];
        for (int i = 0; i < arr1.length; i++) {
            // ищем max
            if (max < arr5[i]) {
                max = arr5[i];
            }
            //ищем min
            if (min > arr5[i]) {
                min = arr5[i];
            }
        }
        System.out.println("max = " + max + " min = " + min);

        System.out.println("6. ** Написать метод, в который передается не пустой одномерный целочисленный массив, метод должен вернуть true, если в массиве есть место, в котором сумма левой и правой части массива равны. Примеры: checkBalance([2, 2, 2, 1, 2, 2, || 10, 1]) → true, checkBalance([1, 1, 1, || 2, 1]) → true, граница показана символами ||, эти символы в массив не входят.");
        int[] arr6 = {1, 1, 0, 0, 8, 0, 1, 1, 0, -1};
        System.out.println("Массив arr6 " + Arrays.toString(arr6));
        System.out.println(leftAndRightSumm(arr6));

        System.out.println(" 7. **** Написать метод, которому на вход подается одномерный массив и число n (может быть положительным, или отрицательным), при этом метод должен сместить все элементымассива на n позиций. Для усложнения задачи нельзя пользоваться вспомогательными массивами.");
        int[] arr7example = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int[] arr7 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int shift;
        System.out.println("Массив arr7 " + Arrays.toString(arr7));
        //проверка
        for (int i = -10; i < 12; i++) {
            spawnArrays(arr7, arr7example); // возвращаем массив в первоночальный вид
            System.out.println("n = " + i + " " + Arrays.toString(arrayShift(i, arr7)));
        }
    }

    public static boolean leftAndRightSumm(int arr[]) {
        int allSumm = arr[0];
        //считаем сумму цифр в массиве
        for (int i = 1; i < arr.length; i++) {
            allSumm += arr[i];
        }
        // Сравнивем части, вычитаея из общей суммы левую часть до i
        int leftSumm = arr[0];
        int tf = 0;
        for (int i = 1; i < arr.length; i++) {
            if (allSumm == allSumm - leftSumm) {
                tf = 1;
            }
            leftSumm += arr[i];
        }
        if (tf == 1) {
            return (true);
        } else {
            return (false);
        }
    }

    public static int[] arrayShift(int n, int[] arr) {
        int m;
        // двигаем вправо если minus = 0, т.е. n было положительным числом
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                // запоминаем первый и ставим на его место хвостовой
                m = arr[0];
                arr[0] = arr[arr.length - 1];
                // дергаем массив за хвост
                for (int j = 1; j < arr.length - 1; j++) {
                    arr[arr.length - j] = arr[arr.length - j - 1];
                }
                // ставим сохраненный элемент в ячейку 1
                arr[1] = m;
            }
        }
        // двигаем влево
        if (n < 0) {
            for (int i = 0; i > n; i--) {
                m = arr[arr.length - 1];
                arr[arr.length - 1] = arr[0];
                for (int j = 1; j < arr.length - 1; j++) {
                    arr[j - 1] = arr[j];
                }
                arr[arr.length - 2] = m;
            }
        }
        return arr;
    }

    public static int[] spawnArrays(int[] arr, int[] arr7example) {

        for (int i = 0; i < arr.length; i++) {
            arr[i] = arr7example[i];
        }
        return arr;
    }

}

