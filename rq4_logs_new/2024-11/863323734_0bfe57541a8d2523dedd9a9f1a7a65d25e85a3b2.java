class Solution {

    public boolean primeSubOperation(int[] nums) {
        int maxVal = getMax(nums); 

        boolean[] primes = generateSieve(maxVal);

        int prev = 1; // The value we are comparing to
        int i = 0;

        while (i < nums.length) {
            int diff = nums[i] - prev; 

            if (diff < 0) {
                return false;
            }

            if (primes[diff] || diff == 0) {
                prev++; 
                i++;
            } else {
                prev++; 
            }
        }

        return true;
    }

    private int getMax(int[] nums) {
        int max = nums[0];
        for (int n : nums) {
            max = Math.max(max, n);
        }
        return max;
    }

    private boolean[] generateSieve(int limit) {
        boolean[] primes = new boolean[limit + 1];
        for (int i = 2; i <= limit; i++) {
            primes[i] = true;
        }

        for (int i = 2; i * i <= limit; i++) {
            if (primes[i]) {
                for (int j = i * i; j <= limit; j += i) {
                    primes[j] = false;
                }
            }
        }

        return primes;
    }
}