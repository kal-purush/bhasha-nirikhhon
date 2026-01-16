package LeetCode;

/**
 * https://leetcode.com/problems/longest-palindromic-substring/
 * 
 * 5. Longest Palindromic Substring
 * Medium
 * 
 * Given a string s, return the longest
 * palindromic
 * substring
 * in s.
 * 
 * 
 * 
 * Example 1:
 * 
 * Input: s = "babad"
 * Output: "bab"
 * Explanation: "aba" is also a valid answer.
 * 
 * Example 2:
 * 
 * Input: s = "cbbd"
 * Output: "bb"
 * 
 * 
 * 
 * Constraints:
 * 
 * 1 <= s.length <= 1000
 * s consist of only digits and English letters.
 */
public class _5_Medium_LongestPalindromicSubstring
{
	public String longestPalindrome(String s)
	{
		int n = s.length();
		String longestPalindrome = null;
		int longestPalindromeLength = 0;
		for(int i = 0; i < n; i++)
		{
			for(int j = n - 1; j >= i; j--)
			{
				if(isPalindrome(s, i, j))
				{
					String subString = s.substring(i, j + 1);
					if(subString.length() > longestPalindromeLength)
					{
						longestPalindromeLength = subString.length();
						longestPalindrome = subString;
						break;
					}
				}
			}
			if(longestPalindromeLength == n)
			{
				break;
			}
		}
		return longestPalindrome;
	}

	private boolean isPalindrome(String s, int startIndex, int endIndex)
	{
		while(startIndex <= endIndex)
		{
			if(s.charAt(startIndex) != s.charAt(endIndex))
			{
				return false;
			}
			startIndex++;
			endIndex--;
		}
		return true;
	}
}