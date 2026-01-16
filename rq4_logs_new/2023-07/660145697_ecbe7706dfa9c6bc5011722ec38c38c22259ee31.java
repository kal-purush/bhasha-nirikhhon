package rish.leets.grind75;

import java.util.List;
import java.util.stream.Collectors;

/*
 * Grind 75 : Week 2
 * 
 * Problem #: 383
 * Problem link : https://leetcode.com/problems/ransom-note/
 * 
 * @author Rishabh Soni
 *
 */
public class RansomNote {

	public static boolean canConstruct(String ransomNote, String magazine) {

		List<Character> notes = ransomNote.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
		List<Character> mags = magazine.chars().mapToObj(e -> (char) e).collect(Collectors.toList());

		for (Character c : notes) {
			if (!mags.contains(c)) {
				return false;
			}
			mags.remove(c);
		}

		return true;
	}

	public static boolean canConstruct2(String ransomNote, String magazine) {

		int[] count = new int[26];
		for (char c : magazine.toCharArray()) {
			count[c - 97]++;
		}

		for (char c : ransomNote.toCharArray()) {
			if (count[c - 97] == 0) {
				return false;
			}
			count[c - 97]--;
		}

		return true;
	}

	public static void main(String[] args) {
		System.out.println(canConstruct2("aa", "aab"));
	}

}