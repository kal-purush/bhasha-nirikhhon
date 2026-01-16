package hw2;


import java.util.Arrays;
import java.util.Random;

public class Main2 {

    static Random random = new Random();

    public static void main(String[] args) {

        invertArr();   // Задание 1
        fillArr();     // задание 2
        changeArr();   // Задание 3
        fillDiagonal();    // Задание 4
        minAndMax();     // Задание 5**

    }

    public static void invertArr() {        // Задание 1
        int[] arr = {1, 0, 0, 1, 0, 1, 1, 1, 0};
        System.out.println(Arrays.toString(arr));
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] > 0) {
                arr[i] = 0;
            } else {
                arr[i] = 1;
            }
        }
        System.out.println(Arrays.toString(arr));
    }

    public static void fillArr() {      // Задание 2
        int[] arr2 = new int[8];
        for (int i = 0; i < arr2.length; i++) {
            int n = i * 3;
            arr2[i] = n;
        }
        System.out.println(Arrays.toString(arr2));
    }

    public static void changeArr() {      // Задание 3
        int[] arr3 = {1, 5, 3, 2, 11, 4, 5, 2, 4, 8, 9, 1};
        System.out.println(Arrays.toString(arr3));
        for (int i = 0; i < arr3.length; i++) {
            if (arr3[i] < 6) {
                arr3[i] *= 2;
            }
        }
        System.out.println(Arrays.toString(arr3));
    }

    public static void fillDiagonal() {     // Задание 4
        int[][] arr4 = new int[4][4];
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                if (arr4[i] == arr4[j] || i + j == 3) {
                    arr4[i][j] = 1;
                }
                System.out.printf("%4d ", arr4[i][j]);
            }
            System.out.println();
        }
    }


    public static void minAndMax() {     // Задание 5**
        int[] arr5 = {-10, -5, 1, 555, 10, 6, 20, 100, 50};
        System.out.println(Arrays.toString(arr5));
        int max = 0;
        int min = 0;
        int n;
        for (int i = 0; i < arr5.length; i++) {
            if (arr5[i] >= max) {
                n = arr5[i];
                max = n;
            }
            if (arr5[i] <= min) {
                n = arr5[i];
                min = n;
            }
        }
        System.out.println("Максимальный элемент: " + max);
        System.out.println("Минимальный элемент: " + min);
    }

//    public static void minAndMax() {   // Заданик 5** с рандомом
//        int[] arr5 = new int[10];
//        int max = 0;
//        int min = 0;
//        int n;
//        for (int i = 0; i < arr5.length; i++) {
//            arr5[i] = random.nextInt(1000) - 500 ;
//            System.out.printf("%1d ", arr5[i]);
//            System.out.println();
//            if (arr5[i] >= max) {
//                n = arr5[i];
//                max = n;
//            }
//            if (arr5[i] <= min) {
//                n = arr5[i];
//                min = n;
//            }
//        }
//        System.out.println("Максимальный элемент: " + max);
//        System.out.println("Минимальный элемент: " + min);
//    }


}
















