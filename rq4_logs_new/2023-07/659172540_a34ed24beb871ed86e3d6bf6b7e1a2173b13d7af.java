package leetCode;

/**
 * Дан массив целых чисел nums, отсортированный в порядке возрастания,
 * и целочисленная цель, напишите функцию для поиска цели в nums.
 * Если цель существует, верните ее индекс. В противном случае вернуть -1.
 * Вы должны написать алгоритм со сложностью выполнения O(log n).
 */

public class BinarySearch {
    public static int binSearch(int[] arr, int target) {

        int left = 0; //левая граница поиска
        int right = arr.length - 1; // правая граница поиска

        while (left <= right) { //left меньше чем right
            int mid = left + (right - left) / 2; //вычисляем середину массива

            if (arr[mid] == target) { //
                return mid;
            } else if (arr[mid] < target) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
   return  -1; //

    }

    public static void main(String[] args) {
        int[] arr = {1,2,3,4,7,8,11,45,33};
        int target = 7;

        int result = binSearch( arr, target);
      if (result !=-1) {
          System.out.println("Значение " + target + " найдено по индексу " + result);
      } else {
          System.out.println("Значение " + target + " не найдено");
      }
    }

}
