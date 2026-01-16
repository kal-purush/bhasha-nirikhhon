using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdventOfCode2021
{
    class Day6
    {
        public static long Run()
        {
            List<string> lines = File.ReadAllLines("Data/Day6Input.txt").ToList();
            List<int> rawFish = lines[0].Split(',').ToList().Select(x => int.Parse(x)).ToList();

            Dictionary<int, long> fish = new Dictionary<int, long>();
            for (int i = 0; i < 9; ++i)
            {
                fish.Add(i, 0);
            }

            foreach (var item in rawFish)
            {
                fish[item] += 1;
            }

            for (int i = 0; i < 256; ++i)
            {
                GrowFish(ref fish);
            }

            return fish.Sum(x => x.Value);
        }

        private static void GrowFish(ref Dictionary<int, long> fish)
        {
            long current = fish[8];
            long next = -1;
            for (int i = 8; i > 0; --i)
            {
                next = fish[i - 1];
                fish[i - 1] = current;
                current = next;
            }

            fish[6] += current;
            fish[8] = current;
        }
    }
}