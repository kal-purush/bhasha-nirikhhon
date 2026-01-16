package my.leetcode;

public class ReverseStringSwap {
	public String reverseString(String s) {
		int size = s.length() - 1;
		char[] chars = s.toCharArray();

		for (int i = 0; i < size / 2; i++) {
			char tmp = chars[size - i];
			chars[size - i] = chars[i];
			chars[i] = tmp;
		}
		return new String(chars);

	}
}