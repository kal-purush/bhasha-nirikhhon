package daohuei.leetcodelizard;

/*
 * Question: Reverse Integer
 * Description: Given a signed 32-bit integer x, return x with its digits reversed. 
 * If reversing x causes the value to go outside the signed 32-bit integer range [-2^31, 2^31 - 1], then return 0.
 * Assume the environment does not allow you to store 64-bit integers (signed or unsigned).
 */
public class ReverseInteger {
    /*
     * Cut Off Unit Digit method
     * 
     * @Author: @daohuei
     * 
     * @Description: We cut off unit digit of x and then put into the result. Then
     * we will multiply the result by 10 in the next round.
     * 
     * @Time: O(n), n = number of x's digits, since we only loop once through the x
     * 
     * @Space: O(1), we only need to store result and prev
     */
    public static int reverse(int x) {
        // initialize result and previous value variable as 0
        int result = 0;
        int prev = 0;

        while (x != 0) {
            // while input x not equal to 0
            // (since when it in unit digit, x/10 = 0, it will stop)
            // put the result to the previous value
            prev = result;
            // we do 10 times of result and then add the unit digit of x
            result = result * 10 + x % 10;
            // remove the unit digit
            x /= 10;
            // if the value of the result after removing the unit digit not equal to prev
            // then it means it is over 32-bit range
            if (result / 10 != prev) {
                return 0;
            }
        }
        // afterward, we will get the reverse number!
        return result;
    }

}