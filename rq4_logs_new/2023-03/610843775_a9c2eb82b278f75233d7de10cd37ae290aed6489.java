/**
 * 1. Задать одномерный массив и найти в нем минимальный и максимальный элементы
 */

package homework1;

public class Task1 {
    public static void main(String[] args) {
        int[] arr = {4, 58, 9, 86, 7, 36, 1};
        int min = arr[0];
        int max = arr[0];
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] > max) max = arr[i];
            if (arr[i] < min) min = arr[i];
            System.out.print(arr[i]+", ");
        }
    System.out.println("\n"+"Min: " + min + "; Маx: " + max);
    }
}