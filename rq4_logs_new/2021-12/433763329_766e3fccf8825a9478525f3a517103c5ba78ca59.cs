using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdventOfCode2021
{
    internal class Cave
    {
        public string Name { get; set; }
        public bool IsBig { get; set; }
        public List<Cave> AdjacentCaves { get; set; } = new List<Cave>();
    }

    internal class Day12
    {
        public static int Run()
        {
            List<Cave> caveMap = new List<Cave>();
            List<string> lines = File.ReadAllLines("Data/Day12Input.txt").ToList();
            foreach (var line in lines)
            {
                var bits = line.Split("-");
                foreach (var bit in bits)
                {
                    if (!caveMap.Any(x => x.Name == bit))
                    {
                        caveMap.Add(new Cave()
                        {
                            Name = bit,
                            IsBig = bit.ToUpper() == bit
                        });
                    }
                }
                var caveA = caveMap.First(x => x.Name == bits[0]);
                var caveB = caveMap.First(x => x.Name == bits[1]);
                caveA.AdjacentCaves.Add(caveB);
                caveB.AdjacentCaves.Add(caveA);
            }

            List<List<Cave>> cavePaths = new List<List<Cave>>();

            var startCave = caveMap.First(x => x.Name == "start");

            cavePaths.Add(new List<Cave> { startCave });
            
            while (cavePaths.Any(x => x.Last().Name != "end"))
            {
                var newPaths = new List<List<Cave>>();

                foreach (var currentPath in cavePaths.Where(x => x.Last().Name != "end"))
                {
                    var currentLast = currentPath.Last(); 
                    bool hasRepeat = currentPath.Where(x => !x.IsBig).Distinct().Count() < currentPath.Where(x => !x.IsBig).Count();
                    foreach (var nextCave in currentLast.AdjacentCaves.Where(x => x.IsBig ||
                      (x.Name != "start" /*&& x.Name != "end"*/) && currentPath.Count(y => y.Name == x.Name) <= (hasRepeat ? 0 : 1)))
                    {
                        var newPath = new List<Cave>();
                        foreach (var item in currentPath)
                        {
                            newPath.Add(item);
                        }
                        newPath.Add(nextCave);
                        newPaths.Add(newPath);
                    }
                }
                cavePaths.RemoveAll(x => x.Last().Name != "end");
                cavePaths.AddRange(newPaths);
            }

            //foreach (var path in cavePaths)
            //{
            //    foreach (var cave in path)
            //    {
            //        Console.Write(cave.Name + " ");
            //    }
            //    Console.Write("\r\n");
            //}
            return cavePaths.Count;
        }
    }
}