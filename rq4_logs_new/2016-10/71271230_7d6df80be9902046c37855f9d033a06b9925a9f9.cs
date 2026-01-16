using Dusyk.DeploymentScriptCreator.Models;
using System.Collections.Generic;
using System.Linq;

namespace Dusyk.DeploymentScriptCreator.Util
{
	public static class Extensions
	{
		public static IEnumerable<InputFile> RecalculateSortOrder(this IEnumerable<InputFile> inputFiles)
		{
			var orderedList = inputFiles.OrderBy(files => files.SortOrder);
			int sortIndex = 0;

			foreach (var file in orderedList)
			{
				file.SortOrder = sortIndex;
				sortIndex++;
			}

			return orderedList;
		}
	}
}