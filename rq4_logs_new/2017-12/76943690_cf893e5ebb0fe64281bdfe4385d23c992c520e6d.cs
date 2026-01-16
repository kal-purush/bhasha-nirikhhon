using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Advent2017.Solutions
{
    class Day12
    {
        public Dictionary<int, HashSet<int>> Paths = new Dictionary<int, HashSet<int>>();

        public void Process(string[] sample)
        {
            var matcher = new Regex(@"(\d+) <-> (.+)");
            Paths = sample.ToDictionary(
                x => Convert.ToInt32(matcher.Match(x).Groups[1].Value),
                x => new HashSet<int>(matcher.Match(x).Groups[2].Value.Split(',').Select(y => Convert.ToInt32(y)))
            );
            
            foreach (var path in Paths)
            {
                var explored = new HashSet<int>();
                path.Value.UnionWith(GetAllLinks(path.Key, explored));
            }
        }

        public HashSet<int> GetAllLinks(int node, HashSet<int> explored)
        {
            var result = new HashSet<int>(Paths[node]);
            foreach (var i in Paths[node])
            {
                if (explored.Contains(i)) continue;
                explored.Add(i);
                result.UnionWith(GetAllLinks(i, explored));
            }
            return result;
        }
        
        public int CountGroups()
        {
            return Paths.GroupBy(x => string.Join("",x.Value.OrderBy(y=>y))).Count();
        }
    }
}