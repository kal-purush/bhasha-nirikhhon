package LeetCode;

/**
 * https://leetcode.com/problems/zigzag-conversion/
 *
 * 6. Zigzag Conversion
 * Medium
 *
 * The string "PAYPALISHIRING" is written in a zigzag pattern on a given number of rows
 * like this: (you may want to display this pattern in a fixed font for better legibility)
 *
 * P   A   H   N
 * A P L S I I G
 * Y   I   R
 *
 * And then read line by line: "PAHNAPLSIIGYIR"
 *
 * Write the code that will take a string and make this conversion given a number of rows:
 *
 * string convert(string s, int numRows);
 *
 *
 *
 * Example 1:
 *
 * Input: s = "PAYPALISHIRING", numRows = 3
 * Output: "PAHNAPLSIIGYIR"
 *
 * Example 2:
 *
 * Input: s = "PAYPALISHIRING", numRows = 4
 * Output: "PINALSIGYAHRPI"
 * Explanation:
 * P     I    N
 * A   L S  I G
 * Y A   H R
 * P     I
 *
 * Example 3:
 *
 * Input: s = "A", numRows = 1
 * Output: "A"
 *
 *
 *
 * Constraints:
 *
 *     1 <= s.length <= 1000
 *     s consists of English letters (lower-case and upper-case), ',' and '.'.
 *     1 <= numRows <= 1000
 */
public class _6_Medium_ZigzagConversion
{
	public String convert(String s, int numRows)
	{
		if(numRows == 1)
		{
			return s;
		}
		int stringLength = s.length();
		Character[][] array = new Character[numRows][stringLength];
		int position = 0, row = 0, column = 0;
		while(position < stringLength)
		{
			array[row][column] = s.charAt(position++);
			if(row < numRows - 1 && column % (numRows - 1) == 0)
			{
				row++;
			}
			else
			{
				row--;
				column++;
			}
		}
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < numRows; i++)
		{
			for(int j = 0; j < stringLength; j++)
			{
				Character ch = array[i][j];
				if(ch != null && !Character.isWhitespace(ch))
				{
					sb.append(ch);
				}
			}
		}
		return sb.toString();
	}
}