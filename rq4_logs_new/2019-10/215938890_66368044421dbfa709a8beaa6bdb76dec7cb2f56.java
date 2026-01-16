/*Time Complexity : O(n)
Space complexity : O(N)
Did it run on leetcode? :yes
Any problems? no
*/
public class Solution {
    public int numberOfArithmeticSlices(int[] A) {
        
        int[] dp = new int[A.length]; // create a dp array to store total slices.
        
        int sum = 0;//initial slices are 0.
        
        for (int i = 2; i < dp.length; i++)
        {
            if (A[i] - A[i - 1] == A[i - 1] - A[i - 2]) // if the difference between current and previous element = diff between the previous 2, it is part of an arithmetic slice. increment count and store in dp array.
            {
                dp[i] = 1 + dp[i - 1];
                sum += dp[i];
            }
        }
        return sum;// return total slices.
    }
}
