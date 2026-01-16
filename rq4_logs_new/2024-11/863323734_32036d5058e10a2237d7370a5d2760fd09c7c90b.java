class Solution {
    public int[] resultsArray(int[] nums, int k) {
        int n = nums.length;
        int[] arr = new int[n - k + 1];

        for (int i = 0; i <= n - k; i++) {
            boolean isConsecutive = true;

            for (int j = 1; j < k; j++) { 
                if (nums[i + j] != nums[i + j - 1] + 1) {
                    isConsecutive = false;
                    break;
                }
            }

            arr[i] = isConsecutive ? nums[i + k - 1] : -1;
        }

        return arr;
    }
}