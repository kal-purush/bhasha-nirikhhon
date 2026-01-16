using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.String
{
	public static class OneAway
	{

		public static bool OneEditAway(string s1, string s2)
		{
			if (s1.Length == s2.Length)
			{
				return OneEditReplaceAway(s1, s2);
			}else if (s1.Length + 1 == s2.Length)
			{
				return OneEditInsertAway(s1, s2);
			}
            else if(s1.Length - 1 == s2.Length)
            {
				return OneEditInsertAway(s2, s1);
            }
            return false;
		}

		public static bool OneEditReplaceAway(string s1, string s2)
		{
			bool result = false;
			for (int i = 0; i < s1.Length; i++)
			{
				if (s1[i] != s2[i]) 
					if(result)
						return false;
				result = true;
			}
			return true;
		}

		public static bool OneEditInsertAway(string s1, string s2)
		{

			int index1 = 0, index2 = 0;
			while (index2 < s2.Length && index1 < s1.Length) {
				if (s1[index1] != s2[index2])
				{
					if (index1 != index2)
						return false;
					index2++;
				}
				else
				{
					index1++;
					index2++;
				}
			}

			return true;
		}

	}
}