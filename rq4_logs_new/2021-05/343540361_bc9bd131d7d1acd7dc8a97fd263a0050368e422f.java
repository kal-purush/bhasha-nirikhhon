package problem45;

public final class JumpGame2 {

    public int jump(final int[] nums) {
        final int n = nums.length;
        int jumps = 0, bestRange = 0, from = 0, to = 0;
        boolean jump;

        while (to < n - 1) {
            jump = false;

            for (int i = from; i <= to; i++) {
                final int range = i + nums[i];
                if (range > bestRange) {
                    bestRange = range;
                    jump = true;
                }
            }

            to = bestRange;

            if (jump)
                ++jumps;
        }

        return jumps;
    }

}