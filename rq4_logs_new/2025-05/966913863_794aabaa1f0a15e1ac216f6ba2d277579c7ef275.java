package leetcode;

public class TwoSum {

    public static void twoSUmCalculate(int[] arr, int target) {
        int i = 0;
        int j = arr.length - 1;
        while (i < j) {
            int sum = arr[i] + arr[j];
            if (sum == target) {
                System.out.println("nums[" + i + "] + nums[" + j + "] = " + target);
                break;
            }
            if (sum > target) {
                j--;
            } else {
                i++;
            }
        }
        System.out.println("[0]");
    }

    public static void main(String[] args) {
        int[] nums = {2, 7, 11, 15};
        int target = 19;
        twoSUmCalculate(nums, target);
    }
}