namespace AocNetLib
{
    public class Day12
    {
        public string Solve(string input)
        {
            var map = ParseInput(input);
            var nav = new Navigation(map);
            var result = nav.CountPaths();
            return result.ToString();
        }

        public string Solve2(string input)
        {
            var map = ParseInput(input);
            var nav = new Navigation(map);
            var result = nav.CountPaths2();
            return result.ToString();
        }

        private Map ParseInput(string input)
        {
            var lines = input.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
            var caves = lines.SelectMany(x=> x.Split(new char[] {'-'})).Distinct().ToList();
            var map = new Map(caves);
            foreach (var line in lines)
            {
                var parts = line.Split(new char[] { '-' });
                var cave0 = map[parts[0]];
                var cave1 = map[parts[1]];
                cave0.Connect(cave1);
            }
            return map;
        }

        class Map
        {
            Dictionary<string, Cave> caves;
            List<Cave> caveList;

            public Cave this[string key] { get => caves[key]; }
            public List<Cave> GetCaves => caveList;

            public readonly Cave Start;
            public readonly Cave End;

            public Map(List<string> caveNames)
            {
                caves = new Dictionary<string, Cave>();
                caveList = new List<Cave>();
                foreach (var name in caveNames)
                {
                    var cave = new Cave(name);
                    caves.Add(name, cave);
                    caveList.Add(cave);
                }
                Start = this["start"];
                End = this["end"];
            }
        }

        class Cave
        {
            public readonly string Name;
            public List<Cave> Connections { get; }
            public readonly bool IsBig;

            public Cave(string name)
            {
                Name = name;
                IsBig = char.IsUpper(name.First());
                Connections = new List<Cave>();
            }

            public void Connect(Cave cave)
            {
                Connections.Add(cave);
                cave.Connections.Add(this);
            }
        }

        class Navigation
        {
            Map map;

            public Navigation(Map map)
            {
                this.map = map;
            }

            public int CountPaths()
            {
                return CountPathsRecursive(new List<Cave>(), map.Start);
            }

            int CountPathsRecursive(List<Cave> visited, Cave start)
            {
                if (start == map.End) return 1;
                int sum = 0;
                if(!start.IsBig) visited.Add(start);
                foreach (var item in start.Connections)
                {
                    if (!visited.Contains(item))
                    {
                        sum += CountPathsRecursive(visited, item);
                    }
                }
                if (!start.IsBig) visited.Remove(start);
                return sum;
            }

            public int CountPaths2()
            {
                return CountPathsRecursive2(new List<Cave>(), map.Start, false);
            }

            int CountPathsRecursive2(List<Cave> visited, Cave start, bool doubled)
            {
                if (start == map.End) return 1;
                int sum = 0;
                if (!start.IsBig) visited.Add(start);
                foreach (var item in start.Connections)
                {
                    if (item != map.Start)
                    {
                        if (!visited.Contains(item))
                        {
                            sum += CountPathsRecursive2(visited, item, doubled);
                        }
                        else if (!doubled && visited.Count(x => x == item) == 1)
                        {
                            sum += CountPathsRecursive2(visited, item, true);
                        }
                    }
                }
                if (!start.IsBig) visited.Remove(start);
                return sum;
            }
        }
    }
}