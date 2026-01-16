package problem1695;

public final class MaximumErasure {

    public int maximumUniqueSubarray(final int[] nums) {
        final int[] posMarks = new int[100_001];

        int max = 0;
        int sum = 0;
        int left = 0;

        for (int i = 0; i < nums.length; i++) {
            final int val = nums[i];
            if (posMarks[val] == 0) {
                posMarks[val] = i + 1;
                sum += val;
                if (max < sum)
                    max = sum;
            } else {
                for (int j = left; j < posMarks[val] - 1; j++) {
                    sum -= nums[j];
                    posMarks[nums[j]] = 0;
                }
                left = posMarks[val];
                posMarks[val] = i + 1;
            }
        }

        return max;
    }

}