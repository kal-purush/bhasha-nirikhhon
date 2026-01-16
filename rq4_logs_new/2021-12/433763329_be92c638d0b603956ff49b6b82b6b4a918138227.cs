using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdventOfCode2021
{
    class Day9
    {
        int xMax = int.MinValue;
        int yMax = int.MinValue;
        public int Run()
        {
            List<string> lines = File.ReadAllLines("Data/Day9Input.txt").ToList();


            Dictionary<Tuple<int, int>, int> map = new Dictionary<Tuple<int, int>, int>();

            int x = 0;
            int y = 0;
            foreach (var line in lines)
            {
                xMax = x - 1;
                x = 0;
                foreach (var cha in line)
                {
                    map.Add(new Tuple<int, int>(x, y), int.Parse(cha.ToString()));
                    ++x;
                }
                ++y;
            }
            yMax = y - 1;


            Dictionary<Tuple<int, int>, int> basinMap = new Dictionary<Tuple<int, int>, int>();

            int minSum = 0;
            foreach (var item in map.Where(x => x.Value != 9))
            {
                var current = item;

                while (!IsMinPoint(current, map))
                {
                    current = StepDown(current, map);
                }

                if (!basinMap.ContainsKey(current.Key))
                {
                    basinMap.Add(current.Key, 0);
                }
                basinMap[current.Key] += 1;
            }

            var max = basinMap.OrderByDescending(x => x.Value).Take(3);

            int sum = 1;
            foreach (var item in max)
            {
                sum *= item.Value;
            }
            return sum;
        }

        private KeyValuePair<Tuple<int, int>, int> StepDown(KeyValuePair<Tuple<int, int>, int> item, Dictionary<Tuple<int, int>, int> map)
        {
            if (item.Key.Item1 > 0 && map[new Tuple<int, int>(item.Key.Item1 - 1, item.Key.Item2)] < map[item.Key])
            {
                return new KeyValuePair<Tuple<int, int>, int>(
                    new Tuple<int, int>(item.Key.Item1 - 1, item.Key.Item2), 
                    map[new Tuple<int, int>(item.Key.Item1 - 1, item.Key.Item2)]);
            }
            if (item.Key.Item1 < xMax && map[new Tuple<int, int>(item.Key.Item1 + 1, item.Key.Item2)] < map[item.Key])
            {
                return new KeyValuePair<Tuple<int, int>, int>(
                     new Tuple<int, int>(item.Key.Item1 + 1, item.Key.Item2),
                     map[new Tuple<int, int>(item.Key.Item1 + 1, item.Key.Item2)]);
            }
            if (item.Key.Item2 > 0 && map[new Tuple<int, int>(item.Key.Item1, item.Key.Item2 - 1)] < map[item.Key])
            {
                return new KeyValuePair<Tuple<int, int>, int>(
                    new Tuple<int, int>(item.Key.Item1, item.Key.Item2 - 1),
                    map[new Tuple<int, int>(item.Key.Item1, item.Key.Item2 - 1)]);
            }
            if (item.Key.Item2 < yMax && map[new Tuple<int, int>(item.Key.Item1, item.Key.Item2 + 1)] < map[item.Key])
            {
                return new KeyValuePair<Tuple<int, int>, int>(
                     new Tuple<int, int>(item.Key.Item1, item.Key.Item2 + 1),
                     map[new Tuple<int, int>(item.Key.Item1, item.Key.Item2 + 1)]);
            }

            throw new Exception();
        }

        private bool IsMinPoint(KeyValuePair<Tuple<int, int>, int> item, Dictionary<Tuple<int, int>, int> map)
        {
            bool isMin = true;
            if (item.Key.Item1 > 0 && map[new Tuple<int, int>(item.Key.Item1 - 1, item.Key.Item2)] <= map[item.Key])
            {
                isMin = false;
            }
            if (item.Key.Item1 < xMax && map[new Tuple<int, int>(item.Key.Item1 + 1, item.Key.Item2)] <= map[item.Key])
            {
                isMin = false;
            }
            if (item.Key.Item2 > 0 && map[new Tuple<int, int>(item.Key.Item1, item.Key.Item2 - 1)] <= map[item.Key])
            {
                isMin = false;
            }
            if (item.Key.Item2 < yMax && map[new Tuple<int, int>(item.Key.Item1, item.Key.Item2 + 1)] <= map[item.Key])
            {
                isMin = false;
            }
            return isMin;
        }
    }
}