using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.String
{
	public static class PalindromePermutation
	{
		public static bool IsPermutationOfPalindrome(string sentence)
		{
			int[] table = BuildFrequencyTable(sentence);
			return CheckMaxOneOdd(table);
		}

		public static bool CheckMaxOneOdd(int[] table)
		{
			bool foundOdd = false;
            for (int i = 0; i < table.Length; i++)
            {
				if (table[i] % 2 == 1)
				{
					if(foundOdd) return false;
					foundOdd = true;
				}
				
			}
			return true;            
		}

		//Map each character to a number. a -> 0, b -> 1, c -> 2
		private static int GetCharNumber(char c)
		{
			int a = 'a';
			int z = 'z';
			int val = c;
			if (a <= val && val <= z)
			{
				return val - a;
			}

			return -1;
		}

		public static int[] BuildFrequencyTable(string sentence)
		{
			int[] table = new int['z' - 'a' + 1];
			foreach (char c in sentence)
			{
				int x = GetCharNumber(c);
				if (x != -1)
					table[x]++;
			}
			return table;
		}

	}
}