class Solution {
    public int rob(int[] nums) {
        int nums_len = nums.length;

        int curr_included = 0;
        int curr_excluded = 0;

        int prev_included = 0;
        int prev_excluded = 0;

        for(int i=0; i<nums_len; i++) {
            curr_included = nums[i] + prev_excluded;
            curr_excluded = Integer.max(prev_included, prev_excluded);

            prev_included = curr_included;
            prev_excluded = curr_excluded;
        }
        return Integer.max(curr_included, curr_excluded);
    }

    public static void main(String args[]) {
        Solution s = new Solution();
        int arr[] = {};
        System.out.println(s.rob(arr));
    }
    
}