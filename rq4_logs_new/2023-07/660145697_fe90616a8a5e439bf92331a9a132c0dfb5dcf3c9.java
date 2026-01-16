package rish.leets.grind.easy;

/*
 * 
 * Problem #: 278
 * Problem link: https://leetcode.com/problems/first-bad-version/
 * 
 * @author Rishabh Soni
 * 
 * Date Attempted: 01/07/2023
 *
 */
public class FirstBadVersion {

	public int firstBadVersion(int n) {

		if (n == 1) {
			return 1;
		}

		int first = 0;
		int last = n - 1;
		int mid;

		while (first < n && last >= 0) {
			mid = first + (last - first) / 2;
			if (isBadVersion(mid)) {
				if (!isBadVersion(mid - 1)) {
					return mid;
				}
				last = mid - 1;
			} else {
				first = mid + 1;
			}
		}
		return n;
	}

	/*
	 * The isBadVersion API is defined in the parent class VersionControl. boolean
	 * isBadVersion(int version);
	 * 
	 * This is a dummy method.
	 */
	private boolean isBadVersion(int n) {
		return true;
	}

}