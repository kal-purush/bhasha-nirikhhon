using System;
using System.Collections.Generic;
using System.Linq;

namespace ListShuffling
{
    public static class TupleExtension
    {
        public static List<string> ShuffleOneByOne(this (List<string>, List<string>) tuple)
        {
            List<string> shuffledList = new List<string>();
            tuple.Item1.Zip(tuple.Item2, (s, s1) => new[] {s, s1}).ToList().ForEach(a => shuffledList.AddRange(a));
            var longerArray = tuple.Item1.Count > tuple.Item2.Count ? tuple.Item1 : tuple.Item2;
            shuffledList.AddRange(longerArray.Skip(shuffledList.Count / 2));
            return shuffledList;
        }

        public static List<string> RandomShuffle(this (List<string>, List<string>) tuple)
        {
            return tuple.Item1.Concat(tuple.Item2).OrderBy(e => Guid.NewGuid()).ToList();
        }
    }
}