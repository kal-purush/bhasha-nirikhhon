using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.String
{
    public static class Urlify
    {
		//1.3 URLify: Write a method to replace all spaces in a string with '%20'. You may assume that the string has sufficient space at the end to hold the additional characters, and that you are given the "true" length of the string.
		// BOOK CRAKING CODING INTERVIEW
		// Time complexity: O(n)
		public static string ReplaceSpaces(char[] str, int trueLength)
		{
			int spaceCount = 0;
			int index = 0;
			for (int i = 0; i < trueLength; i++)
			{
				if (str[i] == ' ')
				{
					spaceCount++;
				}
			}

			index = trueLength + spaceCount * 2;
			if (trueLength < str.Length)
			{
				str[trueLength] = '\0';
			}

			for (int i = trueLength - 1; i >= 0; i--)
			{
				if (str[i] == ' ')
				{
					str[index - 1] = '0';
					str[index - 2] = '2';
					str[index - 3] = '%';
					index -= 3;
				}
				else
				{
					str[index - 1] = str[i];
					index--;
				}
			}

			return new string(str);
		}
	}
}