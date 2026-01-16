class Solution {
    public long maximumSubarraySum(int[] nums, int k) {
        
        int n = nums.length;
        Map<Integer, Integer> count = new HashMap<>();
        long sum = 0, maxSum = 0;

        for (int i = 0; i < k; i++) {
            count.put(nums[i], count.getOrDefault(nums[i], 0) + 1);
            sum += nums[i];
        }

        if (count.size() == k) {
            maxSum = sum;
        }

        for (int i = k; i < n; i++) {
            // Remove the outgoing element
            count.put(nums[i - k], count.get(nums[i - k]) - 1);
            if (count.get(nums[i - k]) == 0) {
                count.remove(nums[i - k]);
            }

            // Add the incoming element
            count.put(nums[i], count.getOrDefault(nums[i], 0) + 1);
            sum = sum + nums[i] - nums[i - k];

            if (count.size() == k) {
                maxSum = Math.max(maxSum, sum);
            }
        }

        return maxSum;
    }
}