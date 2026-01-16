using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.String
{
	public static class StringCompression
	{

		public static string Compressed(string str) 
		{
			StringBuilder sb = new StringBuilder();
			int consecutive = 0;

            for (int i = 0; i < str.Length; i++)
            {
                consecutive++;
				//If next character is different than current, append this char to result
				if (i + 1 >= str.Length || str[i] != str[i + 1])
				{
					sb.Append(str[i]);
					sb.Append(consecutive);
					consecutive = 0;
				}
            }

			return sb.Length < str.Length ? sb.ToString(): str;
        }
	}
}