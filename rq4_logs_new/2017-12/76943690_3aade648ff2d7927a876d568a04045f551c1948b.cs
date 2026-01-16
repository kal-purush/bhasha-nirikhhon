using System;
using System.Collections.Generic;
using System.Linq;

namespace Advent2015.Solutions
{
    class Day9
    {
        public Dictionary<string, Dictionary<string, int>> AdjacencyMatrix = new Dictionary<string, Dictionary<string, int>>();
        public List<string> Cities = new List<string>();

        public void BuildDistances(string[] input)
        {

            Cities = input.Select(x => x.Split(' ')[0])
                .Union(input.Select(x => x.Split(' ')[2])).Distinct().OrderBy(x=>x).ToList();
            
            AdjacencyMatrix = Cities.ToDictionary(x => x, x => Cities.ToDictionary(y => y, y => 0));
            
            foreach (var line in input)
            {
                var parts = line.Split(' ');
                var city1 = parts[0];
                var city2 = parts[2];
                var distance = Convert.ToInt32(parts[4]);
                AdjacencyMatrix[city1][city2] = distance;
                AdjacencyMatrix[city2][city1] = distance;
            }
        }

        public IEnumerable<IEnumerable<T>> GetPermutations<T>(IEnumerable<T> list, int length)
        {
            if (length == 1) return list.Select(t => new T[] { t });

            return GetPermutations(list, length - 1)
                .SelectMany(t => list.Where(e => !t.Contains(e)),
                    (t1, t2) => t1.Concat(new T[] { t2 }));
        }

        public void GeneratePaths()
        {
            var paths = GetPermutations(Cities, Cities.Count);
            foreach (var path in paths)
            {
                Console.WriteLine(string.Join(" -> ", path) + " " + CalculatePathLength(path.ToList()));
            }
        }
        public int CalculatePathLength(List<string> paths)
        {
            int distance = 0;

            for (int i = 0; i != paths.Count - 1; i++)
            {
                distance += AdjacencyMatrix[paths[i]][paths[i + 1]];
            }
            return distance;
            
        }

        public int Part1(string[] input)
        {
            BuildDistances(input);
            var paths = GetPermutations(Cities, Cities.Count);
            return paths.Min(x => CalculatePathLength(x.ToList()));
        }

        public int Part2(string[] input)
        {
            BuildDistances(input);
            var paths = GetPermutations(Cities, Cities.Count);
            return paths.Max(x => CalculatePathLength(x.ToList()));
        }
    }
}