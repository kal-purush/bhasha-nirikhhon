using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AdventOfCode2021
{
    class Day1
    {
        public static int Run()
        {
            var lines = File.ReadAllLines("Data/Day1Input.txt");

            List<int> depths = lines.Select(x => int.Parse(x)).ToList();

            int larger = 0;
            for (int i = 0; i < depths.Count - 3; ++i)
            {
                var currentSum = depths[i] + depths[i + 1] + depths[i + 2];
                var nextSum = depths[i + 1] + depths[i + 2] + depths[i + 3];

                if (nextSum > currentSum)
                {
                    ++larger;
                }
            }

            return larger;
        }
    }
}