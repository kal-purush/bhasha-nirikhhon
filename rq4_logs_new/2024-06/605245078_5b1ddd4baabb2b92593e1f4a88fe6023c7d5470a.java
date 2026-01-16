class Solution {
    public int reverse(int x) {
        if(x == 0) {
            return 0;
        }
        
        boolean isNegative = (x < 0);    
        if(isNegative) {
            x = -1 * x;
        }
        
        long sum = 0;
        while(x > 0) {
            sum = (sum*10) + (x%10);
            x /= 10;
        }
        
        // outside the range on ints
        if(sum > Integer.MAX_VALUE) {
            return 0;   
        }
        
        if(isNegative) {
            return -1 * (int)sum;
        }
        return (int)sum;
    }
}