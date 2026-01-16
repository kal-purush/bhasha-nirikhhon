package problem204;

public final class CountPrimes {

    public int countPrimes(final int n) {
        if (n <= 1)
            return 0;

        final boolean[] nums = new boolean[n];
        int primes = n - 2;

        for (int i = 2; i <= (int) Math.sqrt(n); i++) {
            if (!nums[i])
                for (int j = i + i; j < n; j += i) {
                    if (!nums[j]) {
                        nums[j] = true;
                        primes--;
                    }
                }
        }

        return primes;
    }

}