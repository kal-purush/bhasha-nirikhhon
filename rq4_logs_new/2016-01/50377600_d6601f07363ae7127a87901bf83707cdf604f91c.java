package spy;

/**
 * The default instantiation uses the Sieve of Eratosthenes.
 * 
 * The definition of <i>prime</i> used here: primes are integers greater than one with no positive
 * divisors besides one and itself
 * 
 */
final class DefaultPrimeSource implements PrimeSource {
	/**
	 * 
	 */
	static void validate() {
		DefaultPrimeSource s = new DefaultPrimeSource(true);
		validateLargest(s);
		validateCounts(s);
	}

	/**
	 * @param source
	 */
	private static void validateCounts(DefaultPrimeSource source) {
		System.out.println("Validating count of primes < N");

		// N, #primes < N
		int[][] countingTest = { { -1, 0 }, { 0, 0 }, { 1, 0 }, { 2, 0 }, { 10, 4 }, { 100, 25 },
				{ 1000, 168 }, { 10000, 1229 }, { 100000, 9592 }, { 1000000, 78498 },
				{ 10000000, 664579 }, { 100000000, 5761455 }, { 1000000000, 50847534 } };
		for (int[] test : countingTest) {
			int n = test[0];
			source.getPrimes(n);

			String s = "N = " + n + ", count = " + source.getCount();
			if (source.getCount() == test[1]) {
				System.out.println(s + " passed.");
			} else {
				System.out.println(s + " FAILED.");
			}
		}
	}

	/**
	 * @param source
	 */
	private static void validateLargest(DefaultPrimeSource source) {
		System.out.println("Validating largest prime < N");

		// N, largest prime < N
		int[][] largestTest = { { -1, 0 }, { 0, 0 }, { 1, 0 }, { 2, 0 }, { 10, 7 }, { 100, 97 },
				{ 1000, 997 }, { 10000, 9973 }, { 100000, 99991 }, { 1000000, 999983 },
				{ 10000000, 9999991 }, { 100000000, 99999989 }, { 1000000000, 999999937 } };
		for (int[] test : largestTest) {
			int n = test[0];
			source.getPrimes(n);

			String s = "N = " + n + ", largest = " + source.getCount();
			if (source.getLargest() == test[1]) {
				System.out.println(s + " passed.");
			} else {
				System.out.println(s + " FAILED.");
			}
		}
	}

	private int count;
	private boolean isValidate;
	private int largest;

	/**
	 * 
	 */
	public DefaultPrimeSource() {
	}

	/**
	 * @param isValidate
	 */
	private DefaultPrimeSource(boolean isValidate) {
		this.isValidate = isValidate;
	}

	/**
	 * @return
	 */
	public int getCount() {
		return count;
	}

	/**
	 * @return
	 */
	public int getLargest() {
		return largest;
	}

	@Override
	public int[] getPrimes(int n) {
		int[] result;

		if (n < 2) {
			result = new int[0];
			count = result.length;
		} else {
			int size = n;

			// using isNotPrime to save having to re-initialize the array, all n are initially
			// set to prime (i.e. false)
			boolean[] isNotPrime = new boolean[n];

			isNotPrime[0] = true;
			isNotPrime[1] = true;
			size -= 2;

			for (int i = 2; i * i < n; i++) {
				if (!isNotPrime[i]) {
					for (int j = i; i * j < n; j++) {
						if (!isNotPrime[i * j]) {
							size--;
						}
						isNotPrime[i * j] = true;
					}
				}
			}

			if (isValidate) {
				count = 0;
				largest = 0;
				result = null;
			} else {
				result = new int[size];
			}

			int i = 0;
			int p = 0;
			for (boolean b : isNotPrime) {
				if (!b) {
					if (isValidate) {
						count = ++i;
						largest = p;
					} else {
						result[i++] = p;
					}
				}
				p++;
			}
		}

		return result;
	}
}