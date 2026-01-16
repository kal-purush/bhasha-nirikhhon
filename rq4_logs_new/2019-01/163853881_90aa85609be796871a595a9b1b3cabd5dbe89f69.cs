using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MusicPlayer.Extensions
{
	static class StringExtensions
	{
		public static string StringArrToString(this string[] strArr, string dividerSymbol)
		{
			if (strArr.Count() == 1)
			{
				return strArr[0];
			}
			string outStr = "";
			foreach (var str in strArr)
			{
				outStr = string.Concat(outStr, dividerSymbol, strArr);
			}
			return outStr;
		}
	}
}