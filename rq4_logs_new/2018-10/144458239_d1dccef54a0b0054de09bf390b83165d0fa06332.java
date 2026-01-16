package com.learn.simple;

public class L_796_Rotate_String {

	public boolean rotateString(String A, String B) {
		if (A.length() != B.length()) {
			return false;
		}
		A += A;
		if (A.indexOf(B) == -1) {
			return false;
		}
		return true;
	}
}