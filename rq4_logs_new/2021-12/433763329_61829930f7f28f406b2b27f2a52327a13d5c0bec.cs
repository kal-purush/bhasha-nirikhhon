using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdventOfCode2021
{
    internal class Octo
    {
        public int LightLevel { get; set; }
        public bool Flashed { get; set; }
    }

    internal class Day11
    {
        public static int Run()
        {

            List<string> lines = File.ReadAllLines("Data/Day11Input.txt").ToList();

            Dictionary<Point, Octo> octopuses = new Dictionary<Point, Octo>();

            for (int i = 0; i < lines.Count; i++)
            {
                for (int j = 0; j < lines[i].Length; j++)
                {
                    octopuses.Add(new Point(i, j), new Octo() { LightLevel = int.Parse(lines[i][j].ToString()), Flashed = false });
                }
            }

            int stepCount = 1000000;
            int flashCount = 0;
            for (int i = 0; i < stepCount; ++i)
            {
                foreach (var cephalopod in octopuses)
                {
                    octopuses[cephalopod.Key].LightLevel += 1;
                }
                bool hasFlashed = false;
                do
                {
                    hasFlashed = false;
                    foreach (var cephalopod in octopuses.Where(x => x.Value.LightLevel > 9 && x.Value.Flashed == false))
                    {
                        ++flashCount;
                        hasFlashed = true;
                        for (int x = -1; x <= 1; ++x)
                        {
                            for (int y = -1; y <= 1; ++y)
                            {
                                var pt = new Point(cephalopod.Key.X + x, cephalopod.Key.Y + y);
                                if (octopuses.ContainsKey(pt))
                                {
                                    octopuses[pt].LightLevel += 1;
                                }
                            }
                        }
                        cephalopod.Value.Flashed = true;
                    }
                } while (hasFlashed);

                if (octopuses.All(x => x.Value.Flashed))
                {
                    return i;
                }

                foreach (var cephalopod in octopuses.Where( x => x.Value.Flashed))
                {
                    octopuses[cephalopod.Key].LightLevel = 0;
                    octopuses[cephalopod.Key].Flashed = false;
                }
            }

            return flashCount;

        }
    }
}