using System;
using System.Collections.Generic;
using System.Linq;

namespace Github.Api.Core
{
	public static class SearchApiHelper
	{
		public static string CreateQuery(IEnumerable<string> containingWords, IEnumerable<string> notContainingWords)
		{
			var queryTerms = containingWords.Concat(notContainingWords.SelectMany(GetExcludeResultsDefinition));
			return string.Join("+", queryTerms);
		}

		private static IEnumerable<string> GetExcludeResultsDefinition(string value)
		{
			yield return "NOT";
			yield return value;
		}
	}
}