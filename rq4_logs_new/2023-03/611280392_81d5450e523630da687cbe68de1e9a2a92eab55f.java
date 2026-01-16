package LeetCode;

import java.util.ArrayList;
import java.util.List;

/**
 * https://leetcode.com/problems/longest-substring-without-repeating-characters/
 *
 * 3. Longest Substring Without Repeating Characters
 * Medium
 *
 * Given a string s, find the length of the longest
 * substring
 * without repeating characters.
 *
 *
 *
 * Example 1:
 *
 * Input: s = "abcabcbb"
 * Output: 3
 * Explanation: The answer is "abc", with the length of 3.
 *
 * Example 2:
 *
 * Input: s = "bbbbb"
 * Output: 1
 * Explanation: The answer is "b", with the length of 1.
 *
 * Example 3:
 *
 * Input: s = "pwwkew"
 * Output: 3
 * Explanation: The answer is "wke", with the length of 3.
 * Notice that the answer must be a substring, "pwke" is a subsequence and not a substring.
 *
 *
 *
 * Constraints:
 *
 *     0 <= s.length <= 5 * 104
 *     s consists of English letters, digits, symbols and spaces.
 */
public class _3_Medium_LongestSubstringWithoutRepeatingCharacters
{
	public int lengthOfLongestSubstring(String s)
	{
		int maxLength = 0;
		int n = s.length();
		for(int i = 0; i < n; i++)
		{
			List<Character> subStringList = new ArrayList<>();
			for(int j = i; j < n; j++)
			{
				if(subStringList.contains(s.charAt(j)))
				{
					break;
				}
				subStringList.add(s.charAt(j));
			}
			maxLength = subStringList.size() > maxLength ? subStringList.size() : maxLength;
		}
		return maxLength;
	}

	/*
	Below is one alternative solution
	     public int lengthOfLongestSubstring(String s) {
	         int maxLength = 0;
	         int n = s.length();
	         for(int i=0;i<n;i++)
	         {
	             for(int j=i; j<n;j++)
	             {
	                 boolean hasRepeatedCharacters = checkForRepeatedCharacters(s, i, j);
	                 if(!hasRepeatedCharacters)
	                 {
	                      int newMax = j-i+1;
	                      if(maxLength < newMax)
	                      {
	                          maxLength = newMax;
	                      }
	                     //maxLength = Math.max(maxLength, j - i + 1);
	                 }
	             }
	             maxLength = subStringList.size() > maxLength ? subStringList.size() : maxLength;
	         }
	         return maxLength;
	     }

	 public boolean checkForRepeatedCharacters(String s, int i, int j)
	 {
	     int[] subArray = new int[128];
	     for(;i<=j;i++)
	     {
	         char ch = s.charAt(i);
	         subArray[ch]++;
	         if(subArray[ch] > 1)
	         {
	             return true;
	         }
	     }
	     return false;
	 }
	 */
}