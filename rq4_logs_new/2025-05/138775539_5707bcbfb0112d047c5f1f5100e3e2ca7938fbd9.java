package my.com.app;


// continous sub Array sum divisible by k
// calculate prefix sum and check if the difference of two prefix sums is divisible by k

class Solution {
    public boolean checkSubarraySum(int[] nums, int k) {
        
        int[] prefixSum = new int[nums.length];
        prefixSum[0] = nums[0];
        for (int i =1 ; i< nums.length;i++){
            prefixSum[i] = prefixSum[i - 1] + nums[i];
        }

        for ( int i=1; i<prefixSum.length; i++){
           
            if ( prefixSum[i]%k == 0)
                return true;
        
            for ( int j=i+1; j< prefixSum.length; j++){
                if  ( (prefixSum[j] - prefixSum[i-1]) % k == 0)
                    return true ;
            }
                
        }
        return false;
    }

    // optimizing for prefix sume check, continuous subarray sum is difisible by K 
    // prefixSum[j] - prefixSum[i-1]) % k = 0 
    // that means prefixSum[j] % k = prefixSum[i-1]) % k
    // that means 
    public boolean checkSubarraySumSol2(int[] nums, int k){

        if ( nums.length == 1 )
            return false;

        int[] prefixSum = new int[nums.length];
        prefixSum[0] = nums[0];
        for (int i =1 ; i< nums.length;i++){
            prefixSum[i] = prefixSum[i - 1] + nums[i];
        }

        HashMap<Integer , Integer> hm = new HashMap();

        for ( int i=0 ; i < prefixSum.length; i++){

            int mod = prefixSum[i] % k;
            if ( mod == 0 && i!=0)
                return true;

            if ( hm.containsKey(mod) ){
                
                if ( i - hm.get(mod) > 1 )
                    return true;
            } else {
                hm.put( mod , i);
            }
        }
        return false;
    }

}