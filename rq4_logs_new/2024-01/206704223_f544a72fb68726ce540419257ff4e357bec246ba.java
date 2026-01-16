package com.coding.challenge.binary.search;

class FirstBadVersion {
	FirstBadVersion() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		assert firstBadVersion(2126753390) == 1702766719;
	}

	static int firstBadVersion(int n) {
		int left = 1;
		int right = n;
		while (left < right) {
			final int mid = (int) (((long) left + right) / 2);
			if (isBadVersion(mid))
				right = mid;
			else
				left = mid + 1;
		}
		return left;
	}

	static boolean isBadVersion(int version) {
		return version >= 1702766719;
	}
}