/**
 * 移除元素
 */
class Solution {
    public int removeElement(int[] nums, int val) {
        if (nums.length <= 0) {
            return 0;
        }

        int end = -1;
        for (int i = 0; i < nums.length; ++i) {
            if (nums[i] != val) {
                ++end;
                nums[end] = nums[i];
            }
        }

        return end + 1;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        int[] nums = {2, 2, 2};
        int n = s.removeElement(nums, 2);
        System.out.println(n);

        for (int i = 0; i < n; ++i) {
            System.out.print(nums[i] + " ");
        }
    }
}