package rish.leets.grind.easy;

import java.util.Stack;

public class ValidPalindrome {

	public static boolean isPalindrome(String s) {

		String test = s.replaceAll("[^a-zA-Z0-9]", "");
		if (test.isEmpty()) {
			return true;
		}

		test = test.toLowerCase();

		int length = test.length();
		int mid = length / 2;

		Stack<Character> stack = new Stack<>();

		for (int i = 0; i < length; i++) {
			char c = test.charAt(i);
			if (i < mid) {
				stack.add(c);
			} else if (i > mid) {
				char d = stack.pop();
				if (c != d) {
					return false;
				}
			}
		}

		return true;
	}

	public static void main(String[] args) {
		System.out.println(isPalindrome("A man, a plan, a canal: Panama"));
	}

}