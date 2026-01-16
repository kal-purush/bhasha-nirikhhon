using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace FileAlphanumericCounter
{
    public static class IEnumerableExtension
    {
        public static List<string> FormHighestAndLowestFrequencyContent(this IEnumerable<(char, int)> tuples)
        {
            var lines = new List<string>();
            lines.Add("Top 10:");
            foreach (var valueTuple in tuples.Take(10).ToList())
            {
                lines.Add($"- {valueTuple.Item1} = {valueTuple.Item2}");
            }

            lines.Add("");
            lines.Add("Lowest 10:");
            foreach (var valueTuple in tuples.TakeLast(10).ToList())
            {
                lines.Add($"- {valueTuple.Item1} = {valueTuple.Item2}");
            }

            return lines;
        }
    }
}