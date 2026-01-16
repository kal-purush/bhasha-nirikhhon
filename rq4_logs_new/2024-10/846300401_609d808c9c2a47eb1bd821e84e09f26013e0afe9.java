package podpivasniki.shortfy.site.branchedpipeline;

import java.util.Deque;

public class Algos {

    private static int find(int[] nums){
        int l =0;
        int r =0;
        int maxSum = 0;
        while (r<nums.length){
            if(nums[r] == 1){
                maxSum = Math.max(maxSum, r-l+1);
            }else {
                l = r+1;
            }
            r++;
        }
        return maxSum;
    }


    public static void main(String[] args) {
        System.out.println(find(new int[]{}));

    }
}