package LeetCode;

import java.util.HashMap;
import java.util.Map;

/**
 * https://leetcode.com/problems/roman-to-integer/
 *
 * 13. Roman to Integer
 * Easy
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
 * For example, 2 is written as II in Roman numeral, just two ones added together. 12 is written as XII, which is simply X + II. The number 27 is written as XXVII, which is XX + V + II.
 *
 * Roman numerals are usually written largest to smallest from left to right. However, the numeral for four is not IIII. Instead, the number four is written as IV. Because the one is before the five we subtract it making four. The same principle applies to the number nine, which is written as IX. There are six instances where subtraction is used:
 *
 *     I can be placed before V (5) and X (10) to make 4 and 9.
 *     X can be placed before L (50) and C (100) to make 40 and 90.
 *     C can be placed before D (500) and M (1000) to make 400 and 900.
 *
 * Given a roman numeral, convert it to an integer.
 *
 *
 *
 * Example 1:
 *
 * Input: s = "III"
 * Output: 3
 * Explanation: III = 3.
 *
 * Example 2:
 *
 * Input: s = "LVIII"
 * Output: 58
 * Explanation: L = 50, V= 5, III = 3.
 *
 * Example 3:
 *
 * Input: s = "MCMXCIV"
 * Output: 1994
 * Explanation: M = 1000, CM = 900, XC = 90 and IV = 4.
 *
 *
 *
 * Constraints:
 *
 *     1 <= s.length <= 15
 *     s contains only the characters ('I', 'V', 'X', 'L', 'C', 'D', 'M').
 *     It is guaranteed that s is a valid roman numeral in the range [1, 3999].
 */
public class _13_Easy_RomanToInteger
{
	public int romanToInt(String s)
	{
		int output = 0;
		Map<Character, Integer> metaData = new HashMap<Character, Integer>(){{
			put('I', 1);
			put('V', 5);
			put('X', 10);
			put('L', 50);
			put('C', 100);
			put('D', 500);
			put('M', 1000);
		}};
		int length = s.length();
		for(int i = 0; i< length; i++)
		{
			if(metaData.containsKey(s.charAt(i)))
			{
				int result = metaData.get(s.charAt(i));
				if(i+1 < length && result < metaData.get(s.charAt(i+1)))
				{
					result = metaData.get(s.charAt(i+1)) - result;
					i++;
				}
				output = output + result;
			}
		}
		return output;
		/*
		int output = 0;
		Map<String, Integer> metaData = new HashMap<String, Integer>(){{
			put("I", 1);
			put("IV", 4);
			put("V", 5);
			put("IX", 9);
			put("X", 10);
			put("XL", 40);
			put("L", 50);
			put("XC", 90);
			put("C", 100);
			put("CD", 400);
			put("D", 500);
			put("CM", 900);
			put("M", 1000);
		}};
		int length = s.length();
		int ptr = 0;
		while(ptr < length)
		{
			if(metaData.containsKey(s.substring(ptr, ptr+1)))
			{
				if(ptr+1 < length && metaData.containsKey(s.substring(ptr, ptr+2)))
				{
					output += metaData.get(s.substring(ptr, ptr+2));
					ptr += 2;
				}
				else
				{
					output += metaData.get(s.substring(ptr, ptr+1));
					ptr++;
				}
			}
		}
		return output;
		*/

		/*
		int total = 0;
		int length = s.length();
		for(int j = 0; j < length; j++)
		{
			char ch = s.charAt(j);
			char nextCh = (j + 1 < length) ? s.charAt(j + 1) : ' ';
			switch(ch)
			{
				case 'I':
					switch(nextCh)
					{
						case 'V':
							total = total + 4;
							j++;
							break;
						case 'X':
							total = total + 9;
							j++;
							break;
						default:
							total = total + 1;
					}
					break;
				case 'V':
					total = total + 5;
					break;
				case 'X':
					switch(nextCh)
					{
						case 'L':
							total = total + 40;
							j++;
							break;
						case 'C':
							total = total + 90;
							j++;
							break;
						default:
							total = total + 10;
					}
					break;
				case 'L':
					total = total + 50;
					break;
				case 'C':
					switch(nextCh)
					{
						case 'D':
							total = total + 400;
							j++;
							break;
						case 'M':
							total = total + 900;
							j++;
							break;
						default:
							total = total + 100;
					}
					break;
				case 'D':
					total = total + 500;
					break;
				case 'M':
					total = total + 1000;
					break;
			}
		}
		return total;
		*/
	}
}