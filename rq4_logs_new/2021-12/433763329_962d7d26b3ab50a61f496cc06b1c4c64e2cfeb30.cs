using System.Collections.Generic;
using System.IO;

namespace AdventOfCode2021
{
    class Day2
    {
        public static int Run()
        {
            IEnumerable<string> lines = File.ReadAllLines("Data/Day2Input.txt");

            int depth = 0;
            int pos = 0;
            int aim = 0;
            foreach (var command in lines)
            {
                var bits = command.Split(" ");
                int distance = int.Parse(bits[1]);
                switch (bits[0])
                {
                    case "forward":
                        pos += distance;
                        depth += (distance * aim);
                        break;
                    case "down":
                        aim += distance;
                        break;
                    case "up":
                        aim -= distance;
                        break;
                }
            }
            return depth * pos;
        }
    }
}