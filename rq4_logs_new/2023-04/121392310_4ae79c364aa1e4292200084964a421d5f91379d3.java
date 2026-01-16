//Given an array of integers nums, sort the array in ascending order and return 
//it. 
//
// You must solve the problem without using any built-in functions in O(nlog(n))
// time complexity and with the smallest space complexity possible. 
//
// 
// Example 1: 
//
// 
//Input: nums = [5,2,3,1]
//Output: [1,2,3,5]
//Explanation: After sorting the array, the positions of some numbers are not 
//changed (for example, 2 and 3), while the positions of other numbers are changed (
//for example, 1 and 5).
// 
//
// Example 2: 
//
// 
//Input: nums = [5,1,1,2,0,0]
//Output: [0,0,1,1,2,5]
//Explanation: Note that the values of nums are not necessairly unique.
// 
//
// 
// Constraints: 
//
// 
// 1 <= nums.length <= 5 * 10⁴ 
// -5 * 10⁴ <= nums[i] <= 5 * 10⁴ 
// 
//
// Related Topics Array Divide and Conquer Sorting Heap (Priority Queue) Merge 
//Sort Bucket Sort Radix Sort Counting Sort 👍 4738 👎 683


package leetcode.leetcode.editor.en;

import java.util.Random;

//Java：Sort an Array
public class P912SortAnArray{
    public static void main(String[] args) {
        Solution solution = new P912SortAnArray().new Solution();
        // TO TEST
    }
    //leetcode submit region begin(Prohibit modification and deletion)
class Solution {
        Random rand = new Random();

        // solv2: insert-sort
        public int[] sortArray(int[] nums) {
            insertSort(nums);
            return nums;
        }
        public void insertSort(int[] nums) {
            for (int i = 1; i < nums.length; i++) {
                int tmp = nums[i];
                int j = i;
                while (j > 0 && nums[j-1] > tmp) {
                    nums[j] = nums[j-1];
                    j--;
                }
                nums[j] = tmp;
            }
        }

        // solv1: quick-sort
        public int[] sortArray1(int[] nums) {
            quickSort(nums);
            return nums;
        }
        public void quickSort(int[] arr) {
            quickSort(arr, 0, arr.length - 1);
        }

        private void quickSort(int[] arr, int left, int right) {
            if (left >= right) {
                return;
            }

            int pivot = arr[left+rand.nextInt(right-left+1)];
            int index = partition(arr, left, right, pivot);
            quickSort(arr, left, index - 1);
            quickSort(arr, index, right);
        }

        private int partition(int[] arr, int left, int right, int pivot) {
            while (left <= right) {
                while (arr[left] < pivot) {
                    left++;
                }

                while (arr[right] > pivot) {
                    right--;
                }

                if (left <= right) {
                    swap(arr, left, right);
                    left++;
                    right--;
                }
            }

            return left;
        }

        private void swap(int[] arr, int i, int j) {
            int temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
        }
}
//leetcode submit region end(Prohibit modification and deletion)

}