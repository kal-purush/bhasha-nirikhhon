using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AdventOfCode2021
{
    internal class Day5
    {
        public static int Run()
        {
            List<string> lines = File.ReadAllLines("Data/Day5Input.txt").ToList();

            List<List<int>> grid = new List<List<int>>();
            
            List<Line> gridLines =  new List<Line>();
            foreach (var item in lines)
            {
                gridLines.Add(new Line(item));
            }
            int maxX = gridLines.Max(x => Math.Max(x.StartX, x.EndX));
            int maxY = gridLines.Max(x => Math.Max(x.StartY, x.EndY));
            for (int i = 0; i <= maxX; ++i)
            {
                List<int> row = new List<int>();
                for (int j = 0; j <= maxY; ++j)
                {
                    row.Add(0);
                }
                grid.Add(row);
            }

            foreach (var line in gridLines)
            {
                if (line.StartX == line.EndX)
                {
                    for (int i = Math.Min(line.StartY, line.EndY); i <= Math.Max(line.StartY, line.EndY); ++i)
                    {
                        grid[line.StartX][i] += 1;
                    }
                }
                else if (line.StartY == line.EndY)
                {
                    for (int i = Math.Min(line.StartX, line.EndX); i <= Math.Max(line.StartX, line.EndX); ++i)
                    {
                        grid[i][line.StartY] += 1;
                    }
                }
                else
                {
                    for (int i = line.StartX, j = line.StartY, count = 0; count <= Math.Abs(line.StartX - line.EndX);
                        i = (line.StartX > line.EndX) ? i - 1 : i + 1,
                        j = line.StartY > line.EndY ? j - 1 : j + 1, ++count)
                    {
                        grid[i][j] += 1;
                    }
                }
            }
            int points = 0;
            foreach (var row in grid)
            {
                foreach (var point in row)
                {
                    if (point >= 2)
                    {
                        ++points;
                    }
                }
            }
            return points;
        }
    }
}