package my.com.app;

import java.util.*;

// Climbing Stairs
//base case
// 1. if n == 0 , return 0

class Solution {
    
    // to reach nth stage , no of steps needed will be f(n-1) + f(n-2)
    HashMap<Integer, Integer> hm = new HashMap();
    public int climbStairs(int n) {
        if ( n==0 )
            return 0;
        
        if ( n== 1)
            return 1;
        
        if ( n==2)
            return 2;
        
        if ( !hm.containsKey(n)){
            int steps =  climbStairs(n-1) + climbStairs(n-2);
            hm.put( n , steps );
        }
        
        
        return hm.get(n);
        
        
    }
}