package Easy._217_Contains_Duplicate;

import javax.imageio.stream.ImageInputStream;
import java.util.HashSet;
import java.util.Set;

public class Solution {

//    public static int[] bubbleSort(int[] nums) {
//
//        boolean isSorted = false;
//
//        while (!isSorted) {       // !isSorted = !false = true
//
//            isSorted = true;
//
//            for (int i = 1; i < nums.length; i++) {
//
//                if (nums[i - 1] > nums[i]) {
//                    int temp = nums[i];
//                    nums[i] = nums[i - 1];
//                    nums[i - 1] = temp;
//                    isSorted = false;
//                }
//            }
//        }
//        return nums;
//    }


    public static boolean containsDuplicate(int[] nums) {

        Set<Integer> set = new HashSet<>();
        for (int i = 0; i < nums.length; i++) {
            if (set.contains(nums[i])) {
                return true;
            } else {
                set.add(nums[i]);
            }
        }
        return false;
    }


    public static void main(String[] args) {
        int[] nums = {1, 1, 1, 3, 3, 4, 3, 2, 4, 2};

        System.out.println(containsDuplicate(nums));


    }


}