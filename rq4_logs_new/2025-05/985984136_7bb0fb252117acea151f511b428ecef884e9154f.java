class Solution {
    // time => O(n)
    public int[] getAverages(int[] num, int k) {
        long[] nums = new long[num.length];
        for (int i=0;i<nums.length;i++) {
            nums[i] = num[i];
        }
        int[] ans = new int[nums.length];
        for (int i=1;i<nums.length;i++) {
            nums[i] += nums[i-1];
        }
        int right, left;
        for(int i=0;i<nums.length;i++) {
            left = i-k;
            right = i+k;
            if (left<0  || right >= nums.length) {
                ans[i] = -1;
            } else if (left==right) {
                if (left==0) {
                    ans[i] = (int)nums[i];
                } else {
                    ans[i] = (int)(nums[i]-nums[i-1]);
                }
            } else if (left==0) {
                ans[i] = (int)(nums[right]/((2*k)+1));
            } else {
                ans[i] = (int)((nums[right]-nums[left-1])/((2*k)+1));
            }
        }
        return ans;
    }
}