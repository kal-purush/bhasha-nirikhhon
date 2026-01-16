package HomeWorkApp3;

import java.util.Arrays;

public class HomeWorkApp3 {
    public static void main(String[] args) {
        task1();
        task2();
        task3();
        task4();
        int len = 3;
        int  initialValue = 5;
        int [] arr = task5(len, initialValue);
        System.out.println(Arrays.toString(arr));
        task6();
    }

    static void task1() {
        int[] arr = {0, 1, 1, 1, 0, 0};
        for (int j = 0; j < arr.length; j++) {
            if (arr[j] <= 0) {
                arr[j] = 1;
                System.out.print(arr[j]);
            } else {
                arr[j] = 0;
                System.out.print(arr[j]);

            }

        }
        System.out.println();
    }



    static void task2() {
        int[] arr = new int[100];
        for (int x = 0; x < arr.length; x++) {
            arr[x] = x;
            System.out.println(arr[x] + 1);
        }

    }


    static void task3() {
        int[] arr = {1, 5, 3, 2, 11, 4, 5, 2, 4, 8, 9, 1};
        for (int y = 0; y < arr.length; y++) {
            if (arr[y] < 6) {
                arr[y] = arr[y] * 2;
                System.out.println("Answer" + arr[y]);
            }
        }

    }


    static void task4() {
        int[][] arr = new int[5][5];
        for (int b = 0; b < 5; b++) {
            for (int c = 0; c < 5; c++) {
                if ((b == c) || (b + c == 5 - 1)) {
                    arr[b][c] = 1;
                    System.out.print(arr[b][c]);
                } else {
                    System.out.print(arr[b][c]);
                }
            }
            System.out.println();


        }


    }

     static int [] task5 (int len, int initialValue) {
        int[] arr = new int [len];
        for (int i=0;i< arr.length;i++){
            arr[i]= initialValue;
        }
        return arr;
    }


    static void task6() {
        int[] arr = {2, 9, 3, 15, 7};
        int min = arr[0];
        int max = arr[0];
        for (int i = 0; i < arr.length; i++) {
            if (min > arr[i]) {
                min = arr[i];
            }
            if (max < arr[i]) {
                max = arr[i];
            }

        }
        System.out.print(min+" "+max);
    }
}
