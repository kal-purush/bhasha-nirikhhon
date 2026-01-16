using System;
using System.Collections.Generic;

namespace GigglyLib.ProcGen
{
    internal class AStarPos
    {
        public int X;
        public int Y;
        public int G = int.MaxValue;
        public int F = int.MaxValue;
        public AStarPos CameFrom;
    }

    public class AStar
    {
        private int Heuristic(int x, int y, int endX, int endY)
        {
            return Math.Abs(endX - x) + Math.Abs(endY - y);
        }

        public List<(int x, int y)> GetPath(int startX, int startY, int endX, int endY, int[,] costGraph)
        {
            var closedMap = new AStarPos[costGraph.GetLength(0), costGraph.GetLength(1)];
            var openSet = new List<AStarPos>
            { new AStarPos { X = startX, Y = startY, G = 0, F = Heuristic(startX, startY, endX, endY)} };

            do
            {
                // get lowest fScore in openSet
                AStarPos current = GetLowest(openSet);
                if (current.X == endX && current.Y == endY)
                {
                    return ReconstructPath(current);
                }

                // remove node from openSet
                closedMap[current.X, current.Y] = current;
                openSet.Remove(current);

                foreach (var neighbor in GetAdjacentTiles(current, closedMap))
                {
                    // add neighbour to open set if current path to neighbour is better than previously visited time
                    int tentative_g = current.G + costGraph[neighbor.X, neighbor.Y];
                    if (tentative_g < neighbor.G)
                    {
                        neighbor.CameFrom = current;
                        neighbor.G = tentative_g;
                        neighbor.F = neighbor.G + Heuristic(neighbor.X, neighbor.Y, endX, endY);
                        if (!openSet.Contains(neighbor))
                            openSet.Add(neighbor);
                    }
                }
            } while (openSet.Count > 0);

            return null;
        }

        private AStarPos GetLowest(List<AStarPos> openSet)
        {
            AStarPos output = null;
            int lowestF = int.MaxValue;
            foreach (var pos in openSet)
                if (pos.F < lowestF)
                {
                    lowestF = pos.F;
                    output = pos;
                }

            return output;
        }

        private List<AStarPos> GetAdjacentTiles(AStarPos pos, AStarPos[,] closedMap)
        {
            // need to fill out GScore for the returned AStarPos'
            int width = closedMap.GetLength(0);
            int height = closedMap.GetLength(1);

            var output = new List<AStarPos>();

            if (pos.X - 1 >= 0)
                output.Add(closedMap[pos.X - 1, pos.Y] ?? new AStarPos { X = pos.X - 1, Y = pos.Y, G = int.MaxValue });
            if (pos.X + 1 < width)
                output.Add(closedMap[pos.X + 1, pos.Y] ?? new AStarPos { X = pos.X + 1, Y = pos.Y, G = int.MaxValue });
            if (pos.Y - 1 >= 0)
                output.Add(closedMap[pos.X, pos.Y - 1] ?? new AStarPos { X = pos.X, Y = pos.Y - 1, G = int.MaxValue });
            if (pos.Y + 1 < height)
                output.Add(closedMap[pos.X, pos.Y + 1] ?? new AStarPos { X = pos.X, Y = pos.Y + 1, G = int.MaxValue });

            return output;
        }

        private List<(int x, int y)> ReconstructPath(AStarPos end)
        {
            var path = new List<AStarPos>();

            AStarPos current = end;
            while (current != null)
            {
                path.Add(current);
                current = current.CameFrom;
            }

            var output = new List<(int x, int y)>();
            foreach (var element in path)
                output.Add((element.X, element.Y));
            return output;
        }
    }
}