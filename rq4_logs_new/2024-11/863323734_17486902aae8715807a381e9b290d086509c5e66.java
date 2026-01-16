class Solution {
    public int minimumSubarrayLength(int[] nums, int k) {
        int n = nums.length;
        int len = n+1;
        int bits[] = new int[32];
        int xor = 0;

        for(int i = 0, j = 0; j < n; j++){
            xor |= nums[j];

            for(int h = 0; h < 32; h++){
                if((nums[j] >> h & 1) == 1){
                    bits[h]++;
                }
            } 

            while(xor >= k && i <= j){
                len = Math.min(len, j - i + 1);

                for(int h = 0; h < 32; h++){
                    if((nums[i] >> h & 1) == 1){
                        bits[h]--;
                        if(bits[h] == 0){
                            xor ^= 1 << h;
                        }
                    }
                }
                i++;
            }
            
        }

        return len > n ? -1 : len;
    }
}