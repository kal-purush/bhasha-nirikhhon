/*
        In a country popular for train travel, you have planned some train travelling one year in advance.  
        The days of the year that you will travel is given as an array days.  
        Each day is an integer from 1 to 365.

        Train tickets are sold in 3 different ways:

        a 1-day pass is sold for costs[0] dollars;
        a 7-day pass is sold for costs[1] dollars;
        a 30-day pass is sold for costs[2] dollars.
        The passes allow that many days of consecutive travel.  For example, if we get a 7-day pass on day 2, then we can travel for 7 days: day 2, 3, 4, 5, 6, 7, and 8.

        Return the minimum number of dollars you need to travel every day in the given list of days.

        

        Example 1:
        Input: days = [1,4,6,7,8,20], costs = [2,7,15]
        Output: 11
        Explanation: 
        For example, here is one way to buy passes that lets you travel your travel plan:
        On day 1, you bought a 1-day pass for costs[0] = $2, which covered day 1.
        On day 3, you bought a 7-day pass for costs[1] = $7, which covered days 3, 4, ..., 9.
        On day 20, you bought a 1-day pass for costs[0] = $2, which covered day 20.
        In total you spent $11 and covered all the days of your travel.

        Example 2:
        Input: days = [1,2,3,4,5,6,7,8,9,10,30,31], costs = [2,7,15]
        Output: 17
        Explanation: 
        For example, here is one way to buy passes that lets you travel your travel plan:
        On day 1, you bought a 30-day pass for costs[2] = $15 which covered days 1, 2, ..., 30.
        On day 31, you bought a 1-day pass for costs[0] = $2 which covered day 31.
        In total you spent $17 and covered all the days of your travel.
        

        Note:

        1 <= days.length <= 365
        1 <= days[i] <= 365
        days is in strictly increasing order.
        costs.length == 3
        1 <= costs[i] <= 1000
*/
class Solution {
        public int mincostTickets(int[] days, int[] costs) {
                // return helper(days, 0, costs, 0);
                return helperTopDown(days, costs);
        }
        
        private int helper(int[] days, int index, int[] costs, int end){
                if(index == days.length){
                        return 0;
                }
                
                if(end >= days[index]){
                        return helper(days, index+1, costs, end);
                }
                
                // int min = Integer.MAX_VALUE;
                int one     = costs[0] + helper(days, index, costs, days[index]);
                int seven   = costs[1] + helper(days, index, costs, days[index] + 6);
                int month   = costs[2] + helper(days, index, costs, days[index] + 29);
                return Math.min(one, Math.min(seven, month));
        }
        
         private int helperTopDown(int[] days, int[] costs){
                int lastDay          = days[days.length-1];
                int[] dp             = new int[lastDay+1];
                boolean[] travelDays = new boolean[lastDay+1];
                for(int i = 0; i < days.length; i++){
                        travelDays[days[i]] = true;
                }
                
                for(int day = 1; day <= lastDay; day++){
                        if(!travelDays[day]){
                                dp[day] = dp[day-1];
                                continue;
                        }
                        int c1  = costs[0] + dp[day-1];
                        int c2  = costs[1] + dp[Math.max(day-7,0)];
                        int c3  = costs[2] + dp[Math.max(day-30, 0)];
                        dp[day] = Math.min(Math.min(c1,c2),c3);
                }
                return dp[lastDay];
        }
}