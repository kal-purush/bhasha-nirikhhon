using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdventOfCode2021
{
    class Day7
    {
        public static int Run()
        {
            List<string> lines = File.ReadAllLines("Data/Day7Input.txt").ToList();
            List<int> startPositions = lines.First().Split(',').Select(x => int.Parse(x)).ToList();

            int lowSum = int.MaxValue;
            int bestPosition = 0;

            for (int i = startPositions.Min(); i < startPositions.Max(); ++i)
            {
                int currentSum = 0;
                foreach (var item in startPositions)
                {
                    int diff = Math.Abs(i - item);
                    currentSum += nthTriangle(diff);
                }

                if (currentSum < lowSum)
                {
                    lowSum = currentSum;
                    bestPosition = i;
                }
            }

            return lowSum;
        }

        private static int nthTriangle(int diff)
        {
            int res = 0;
            for (int i = 1; i <= diff; ++i)
            {
                res += i;
            }
            return res;
        }
    }
}