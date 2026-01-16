package rish.leets.contests.weekly;

/*
 *            Weekly Contest 351
 * 
 * Problem #: 2748
 * Problem link: https://leetcode.com/problems/number-of-beautiful-pairs
 * Contest link: https://leetcode.com/contest/weekly-contest-351/
 * 
 * Date Attempted: 28/06/2023
 * 
 * @author Rishabh Soni
 * 
 */
public class CountBeautifulPairs {

    public int countBeautifulPairs(int[] nums) {

        int length = nums.length;
        int[] first = new int[length];
        int[] last = new int[length];

        for (int i = 0; i < length; i++) {
            last[i] = lastDigit(nums[i]);
            first[i] = firstDigit(nums[i]);
        }

        int count = 0;

        for (int i = 0; i < length; i++) {
            for (int j = i + 1; j < length; j++) {
                if (isCoPrime(first[i], last[j])) {
                    count++;
                }
            }
        }

        return count;
    }

    private int firstDigit(int n) {
        while (n >= 10) {
            n /= 10;
        }
        return n;
    }

    private int lastDigit(int n) {
        return n % 10;
    }

    private boolean isCoPrime(int a, int b) {
        return gcd(a, b) == 1;
    }

    public int gcd(int a, int b) {
        return b == 0 ? a : gcd(b, a % b);
    }

}