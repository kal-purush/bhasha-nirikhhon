package LeetCode_Daily;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

public class Set_Mismatch {

    public static void main(String[] args) {

        int[] arr = {1, 5, 3, 2, 2, 7, 6, 4, 8, 9};
        System.out.println(Arrays.toString(findErrorNums(arr)));

    }

    public static int[] findErrorNums(int[] nums) {
        Arrays.sort(nums);

        int missingValue = nums.length;
        int duplicateValue = 0;

        HashSet<Integer> hashSet = new HashSet<>();
        for (int value : nums) {
            hashSet.add(value);
        }

        Iterator iterator = hashSet.iterator();

        for (int i = 0; i < hashSet.size(); i++) {
            int element = (int) iterator.next();
            if (i + 1 != element) {
                missingValue = i + 1;
                break;
            }
        }

        for (int i = 1; i < nums.length; i++) {
            if (nums[i] == nums[i - 1]) {
                duplicateValue = nums[i];
            }
        }

        int[] outputArray = {duplicateValue, missingValue};

        return outputArray;
    }

}

