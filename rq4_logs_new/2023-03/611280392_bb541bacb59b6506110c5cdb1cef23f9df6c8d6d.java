package LeetCode;

/**
 * https://leetcode.com/problems/longest-common-prefix/
 * 
 * 14. Longest Common Prefix
 * Easy
 * 
 * Write a function to find the longest common prefix string amongst an array of strings.
 * 
 * If there is no common prefix, return an empty string "".
 * 
 * 
 * 
 * Example 1:
 * 
 * Input: strs = ["flower","flow","flight"]
 * Output: "fl"
 * 
 * Example 2:
 * 
 * Input: strs = ["dog","racecar","car"]
 * Output: ""
 * Explanation: There is no common prefix among the input strings.
 * 
 * 
 * 
 * Constraints:
 * 
 * 1 <= strs.length <= 200
 * 0 <= strs[i].length <= 200
 * strs[i] consists of only lowercase English letters.
 */
public class _14_Longest_Common_Prefix
{
	public static void main(String[] args)
	{
		System.out.println(longestCommonPrefix(new String[] {"flower", "flow", "flight"}));
		System.out.println(longestCommonPrefix(new String[] {"dog", "racecar", "car"}));
		//System.out.println(longestCommonPrefix(new String[]{"flower","flow","flight"}));
	}

	public static String longestCommonPrefix(String[] strs)
	{
		int stringIterator = 0;
		String firstStr = strs[0];
		int firstStrLength = firstStr.length();
		for(; stringIterator < firstStrLength; stringIterator++)
		{
			Character firstStrCh = firstStr.charAt(stringIterator);
			boolean mismatchFound = false;
			for(int arrayIterator = 1; arrayIterator < strs.length; arrayIterator++)
			{
				String str = strs[arrayIterator];
				if(stringIterator >= str.length() || str.charAt(stringIterator) != firstStrCh)
				{
					mismatchFound = true;
					break;
				}
			}
			if(mismatchFound)
			{
				break;
			}
		}
		return firstStr.substring(0, stringIterator);
	}
}