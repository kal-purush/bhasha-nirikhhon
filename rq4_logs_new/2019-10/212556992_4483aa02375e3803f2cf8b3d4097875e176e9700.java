/**
 * 删除数组中的重复项
 * 仅在原来的数组上操作，并返回不重复的个数
 */
class Solution {
    public int removeDuplicates(int[] nums) {
        if (nums.length <= 0) {
            return 0;
        }

        int end = 0;
        int curr = nums[end];
        for (int i = 1; i < nums.length; ++i) {
            if (curr == nums[i]) {
                continue;
            }

            ++end;
            if (i == end) {
                curr = nums[end];
            }
            else {
                nums[end] = nums[i];
                curr = nums[end];
            }
        }

        System.out.println("end=" + end);
        return end + 1;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        int[] nums = {0,0,1,1,1,2,2,3,3,4};
        int n = s.removeDuplicates(nums);
        System.out.println(n);

        for (int i = 0; i < n; ++i) {
            System.out.print(nums[i] + " ");
        }
    }
}