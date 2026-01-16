using System;
using System.Linq;

namespace Advent2017.Solutions
{
    class Day2
    {
        public int Solve(string[] input)
        {
            return input.Sum(x => {
                var line = x.Split('\t').Select(Int32.Parse).ToList();
                return line.Max() - line.Min();
            });
        }

        public int Solve2(string[] input)
        {
            return input.Sum(x => {
                var numbers = x.Split('\t').Select(Int32.Parse).ToList();
                return (from a in numbers
                    from b in numbers
                    where a != b
                          && a % b == 0
                    select a / b).Single();
            });
        }
    }
}