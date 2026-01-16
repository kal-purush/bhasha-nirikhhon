using QuickGraph;
using QuickGraph.Algorithms;
using QuickGraph.Algorithms.ShortestPath;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdventOfCode2021
{

    internal struct AisPoint : IComparable, IEquatable<AisPoint>
    {
        public int X { get; set; }
        public int Y { get; set; }
        public AisPoint(int x, int y)
        {
            X = x;
            Y = y;
        }
        public int CompareTo(object obj)
        {
            if (obj is AisPoint)
            {
                var foo = (AisPoint)obj;
                {
                    if ( (X + Y) < foo.X + foo.Y)
                    {
                        return -1;
                    }
                    if ((X + Y) == foo.X + foo.Y)
                    {
                        return 0;
                    }
                    return 1;
                }
            }
            return 0;
        }

        public bool Equals(AisPoint other)
        {
            return X == other.X && Y == other.Y;
        }
    }


    class Day15
    {
        public static int Run()
        {
            List<string> lines = File.ReadAllLines("Data/Day15Input.txt").ToList();
            Dictionary<AisPoint, int> smallCaveMap = new Dictionary<AisPoint, int>();
            int x = 0;
            int y = 0;
            foreach (var line in lines)
            {
                x = 0;
                foreach (var cha in line)
                {
                    smallCaveMap.Add(new AisPoint(x, y), int.Parse(cha.ToString()));
                    ++x;
                }
                ++y;
            }
            Dictionary<AisPoint, int> caveMap = new Dictionary<AisPoint, int>();
            int size = x;
            for (int i = 0; i < 5; ++i)
            {
                for (int j = 0; j < 5; ++j)
                {
                    foreach (var pt in smallCaveMap)
                    {
                        var value = (pt.Value + i + j);
                        while (value > 9) { value -= 9; }
                        caveMap.Add(new AisPoint(pt.Key.X + (size * i), pt.Key.Y + (size * j)), value);
                    }
                }
            }

            AdjacencyGraph<AisPoint, Edge<AisPoint>> graph = new AdjacencyGraph<AisPoint, Edge<AisPoint>>(true);
            foreach (var pt in caveMap)
            {
                graph.AddVertex(pt.Key);
            }

            foreach (var pt in caveMap)
            {

                // x -1
                var nextAisPoint = new AisPoint(pt.Key.X - 1, pt.Key.Y);
                if (caveMap.ContainsKey(nextAisPoint))
                {
                    var edge = new Edge<AisPoint>(pt.Key, nextAisPoint);
                    graph.AddEdge(edge);
                }
                // x + 1
                nextAisPoint = new AisPoint(pt.Key.X + 1, pt.Key.Y);
                if (caveMap.ContainsKey(nextAisPoint))
                {
                    var edge = new Edge<AisPoint>(pt.Key, nextAisPoint);
                    graph.AddEdge(edge);
                }
                // y + 1
                nextAisPoint = new AisPoint(pt.Key.X, pt.Key.Y - 1);
                if (caveMap.ContainsKey(nextAisPoint))
                {
                    var edge = new Edge<AisPoint>(pt.Key, nextAisPoint);
                    graph.AddEdge(edge);
                }
                nextAisPoint = new AisPoint(pt.Key.X, pt.Key.Y + 1);
                if (caveMap.ContainsKey(nextAisPoint))
                {
                    var edge = new Edge<AisPoint>(pt.Key, nextAisPoint);
                    graph.AddEdge(edge);
                }
            }

            Func<Edge<AisPoint>, double> edgeCostFunct = e => caveMap[e.Target];

            AisPoint root = new AisPoint(0, 0);

            // compute shortest paths
            var tryGetPaths = graph.ShortestPathsDijkstra(edgeCostFunct, root);

            // query path for given vertices
            AisPoint targetPoint = caveMap.Keys.Max();// pt => pt.X + pt.Y);
            int sum = 0;
            IEnumerable<Edge<AisPoint>> path;
            if (tryGetPaths(targetPoint, out path))
            {
                foreach (var edge in path)
                {
                    sum += caveMap[edge.Target];
                }
            }


            return sum;
        }
    }
}