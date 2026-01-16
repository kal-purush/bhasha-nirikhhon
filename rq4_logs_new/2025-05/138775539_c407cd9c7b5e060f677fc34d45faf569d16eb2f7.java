class Solution {
    public int findMiddleIndex(int[] nums) {
        
        int[] sum = new int[nums.length];

        for (int i=0 ;i < nums.length; i++){
            
            sum[i] = (i == 0)? nums[i] : nums[i] + sum[i-1];
        }

        int len = sum.length;

        for (int i=0 ;i < len; i++){
            
            if (i==0){
                if (sum[len-1] - sum[i] == 0) 
                    return i;
            } else {
                if ( sum[i-1] == sum[len-1] - sum[i]){
                    return i;
                }

                if ( (i == len-1) &&  sum[len-2] == 0 ){
                    return i;
                }
            }
        }
        return -1;
    }
}