package LeetCode;

import java.util.Stack;

/**
 * https://leetcode.com/problems/integer-to-roman/
 *
 * 12. Integer to Roman
 * Medium
 *
 * Roman numerals are represented by seven different symbols: I, V, X, L, C, D and M.
 *
 * Symbol       Value
 * I             1
 * V             5
 * X             10
 * L             50
 * C             100
 * D             500
 * M             1000
 *
 * For example, 2 is written as II in Roman numeral, just two one's added together.
 * 12 is written as XII, which is simply X + II.
 * The number 27 is written as XXVII, which is XX + V + II.
 *
 * Roman numerals are usually written largest to smallest from left to right.
 * However, the numeral for four is not IIII.
 * Instead, the number four is written as IV.
 * Because the one is before the five we subtract it making four.
 * The same principle applies to the number nine, which is written as IX.
 * There are six instances where subtraction is used:
 *
 *     I can be placed before V (5) and X (10) to make 4 and 9.
 *     X can be placed before L (50) and C (100) to make 40 and 90.
 *     C can be placed before D (500) and M (1000) to make 400 and 900.
 *
 * Given an integer, convert it to a roman numeral.
 *
 *
 *
 * Example 1:
 *
 * Input: num = 3
 * Output: "III"
 * Explanation: 3 is represented as 3 ones.
 *
 * Example 2:
 *
 * Input: num = 58
 * Output: "LVIII"
 * Explanation: L = 50, V = 5, III = 3.
 *
 * Example 3:
 *
 * Input: num = 1994
 * Output: "MCMXCIV"
 * Explanation: M = 1000, CM = 900, XC = 90 and IV = 4.
 *
 *
 *
 * Constraints:
 *
 *     1 <= num <= 3999
 */
public class _12_IntegerToRoman
{
	public String intToRoman(int num)
	{
		int nums[] = new int[] {1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1};
		String romans[] = new String[] {"M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"};
		StringBuilder output = new StringBuilder();
		for(int i = 0; i < nums.length; i++)
		{
			while(num >= nums[i])
			{
				output.append(romans[i]);
				num = num - nums[i];
			}
		}
		return output.toString();
		/**
		 * Below is my initial solution which took more space and time.
		 */
		/*
		String[][] metaData = new String[][]
			{
				{"I", "IV", "V", "IX"},
				{"X", "XL", "L", "XC"},
				{"C", "CD", "D", "CM"},
				{"M"}
			};
		Stack<String> outputStack = new Stack<>();
		int places = 0;
		while(num > 0)
		{
			StringBuilder sb = new StringBuilder();
			int rem = num % 10;
			num = num / 10;
			String[] currentMeta = metaData[places];
			if(rem == 9)
			{
				sb.append(currentMeta[3]);
				rem -= 9;
			}
			else if(rem >= 5)
			{
				sb.append(currentMeta[2]);
				rem -= 5;
			}
			else if(rem == 4)
			{
				sb.append(currentMeta[1]);
				rem -= 4;
			}
			while(rem > 0)
			{
				sb.append(currentMeta[0]);
				rem--;
			}
			places++;
			outputStack.push(sb.toString());
		}
		StringBuilder outputStr = new StringBuilder();
		while(!outputStack.isEmpty())
		{
			outputStr.append(outputStack.pop());
		}
		return outputStr.toString();
		*/
	}
}