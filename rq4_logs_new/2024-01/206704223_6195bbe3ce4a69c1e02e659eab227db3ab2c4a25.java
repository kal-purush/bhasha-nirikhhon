package com.coding.challenge.simulation;

class DataStream {
	private final int k;
	private final int value;
	int lastNotMatching = 0;
	int curr = 0;

	DataStream(int value, int k) {
		this.k = k;
		this.value = value;
	}

	public static void main(String[] args) {
		final DataStream dataStream = new DataStream(4, 3); // value = 4, k = 3
		assert !dataStream.consec(4); // Only 1 integer is parsed, so returns False.
		assert !dataStream.consec(4); // Only 2 integers are parsed. Since 2 is less than k, returns False.
		assert dataStream.consec(4); // The 3 integers parsed are all equal to value, so returns True.
		/*
		 * The last k integers parsed in the stream are [4,4,3]. Since 3 is not equal to
		 * value, it returns False.
		 */
		assert !dataStream.consec(3);
	}

	public boolean consec(int num) {
		curr = curr + 1;
		if (num != value)
			lastNotMatching = curr;
		return curr - lastNotMatching >= k;
	}
}