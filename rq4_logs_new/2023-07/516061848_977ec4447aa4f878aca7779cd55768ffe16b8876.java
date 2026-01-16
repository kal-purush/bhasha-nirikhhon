package Easy._217_Contains_Duplicate;

import java.util.Arrays;

public class Test {


    public static int[] bubbleSort(int[] nums) {

        boolean isSorted = false;

        while (!isSorted) {       // !isSorted = !false = true

            isSorted = true;

            for (int i = 1; i < nums.length; i++) {

                if (nums[i - 1] > nums[i]) {
                    int temp = nums[i];
                    nums[i] = nums[i - 1];
                    nums[i - 1] = temp;
                    isSorted = false;
                }
            }
        }
        return nums;
    }


    public static void main(String[] args) {
        int[] array = new int[]{64, 42, 73, 41, 32, 53, 16, 24, 57, 42, 74, 55, 36};
        bubbleSort(array);
        System.out.println(Arrays.toString(array));
    }

}