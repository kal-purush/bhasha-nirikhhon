package lesson5.zd3;

/**
 * Задача 3:
 * Найдите сумму элементов на главной диагонали
 */

public class Zd3 {

    public static void main(String[] args) {

        int[][] arr = {{5, 2, 5, 8, 9}, {1, 2, 4, 5, 6}, {1, 2, 1, 2, 1}, {0, 12, 1, 4, 5}, {7, 8, 9, 2, 3}};
        int sumGl = 0;
        int sumDop = 0;
        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr[i].length; j++) {
                System.out.print(arr[i][j] + " ");
                if (arr[i] == arr[j]) {
                    sumGl += arr[i][j];
                }
            }
            System.out.println();
        }
        System.out.println("Сумма главной ветки = " + sumGl);

        for (int i = 0; i < arr.length; i++) {
            sumDop += arr[i][arr.length - 1 - i];
        }
        System.out.println("Сумма побочной ветки = " + sumDop);
    }

}