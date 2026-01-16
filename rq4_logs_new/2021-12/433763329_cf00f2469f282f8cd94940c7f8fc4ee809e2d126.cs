using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AdventOfCode2021
{
    class Day3
    {
        public static int Run()
        {
            List<string> lines = File.ReadAllLines("Data/Day3Input.txt").ToList();

            List<List<int>> convertedLines = new List<List<int>>();

            foreach (var item in lines)
            {
                List<int> current = new List<int>();
                foreach (var foo in item)
                {
                    current.Add(int.Parse(foo.ToString()));
                }
                convertedLines.Add(current);
            }
            var leastCommon = convertedLines;

            var mostCommon = convertedLines;
            int currentBit = 0;
            while (leastCommon.Count > 1)
            {
                leastCommon = FilterList(leastCommon, currentBit, false);
                ++currentBit;
            }

            currentBit = 0;
            while (mostCommon.Count > 1)
            {
                mostCommon = FilterList(mostCommon, currentBit, true);
                ++currentBit;
            }

            int mostCommonValue = BitToInt(mostCommon.First());
            int leastCommonValue = BitToInt(leastCommon.First());
            return mostCommonValue * leastCommonValue;
        }

        private static int BitToInt(List<int> bits)
        {
            int value = 0;
            int current = 1;
            for (int i = 1; i <= bits.Count; ++i)
            {
                value += current * bits[bits.Count - i];
                current *= 2;
            }
            return value;
        }

        private static List<List<int>> FilterList(List<List<int>> input, int bit, bool mostCommon)
        {
            int zeroCount = input.Count(x => x[bit] == 0);
            int keepValue = -1;
            if (zeroCount == input.Count / 2)
            {
                keepValue = mostCommon ? 1 : 0;
            }
            else if (zeroCount > (input.Count / 2))
            {
                keepValue = mostCommon ? 0 : 1;
            }
            else
            {
                keepValue = mostCommon ? 1: 0;
            }

            return input.Where(x => x[bit] == keepValue).ToList();

        } 

    }
}