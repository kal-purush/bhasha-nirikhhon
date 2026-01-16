using System;
using System.Collections.Generic;

namespace GigglyLib.ProcGen
{
    public class BSPSplit
    {
        public int X1;
        public int Y1;
        public int X2;
        public int Y2;
    }

    public class BSPGenerator
    {
        public void Generate(bool[,] tiles)
        {
            SplitRegion(tiles);
        }
    
        public BSPSplit SplitRegion(bool[,] region)
        {
            bool[,] clonedRegion = (bool[,])region.Clone();
            for (int x = 0; x < clonedRegion.GetLength(0); x++)
            {
                for (int y = 0; y < clonedRegion.GetLength(1); y++)
                    clonedRegion[x, y] = false;

                var tileCount = GetTileCount(clonedRegion);
                clonedRegion = (bool[,])region.Clone();
            }
            return null;
        }

        private (int first, int second) GetTileCount(bool[,] regions)
        {
            (int first, int second) counts = (int.MinValue, int.MinValue);

            for (int x = 0; x < regions.GetLength(0); x++)
            {
                for (int y = 0; y < regions.GetLength(1); y++) 
                {
                    if (regions[x, y])
                    {
                        int tiles = FloodFill(regions, x, y);
                        if (tiles > counts.first)
                        {
                            counts.second = counts.first;
                            counts.first = tiles;
                        }
                        else if (tiles > counts.second)
                        {
                            counts.second = tiles;
                        }
                    }
                } 
            }

            return counts;
        }

        private int FloodFill(bool[,] region, int x, int y)
        {
            int tileCount = 0;
            List<(int x, int y)> openSet = new List<(int x, int y)>{
                (x, y),
            };

            while (openSet.Count > 0)
            {
                (x, y) = openSet[0];
                if (region[x, y])
                {
                    region[x, y] = false;
                    tileCount++;
                    openSet.RemoveAt(0);

                    if (x + 1 < region.GetLength(0))
                        openSet.Add((x + 1, y));
                    if (x - 1 >= 0)
                        openSet.Add((x - 1, y));
                    if (y + 1 < region.GetLength(1))
                        openSet.Add((x, y + 1));
                    if (y - 1 >= 0)
                        openSet.Add((x, y - 1));
                }
                else
                {
                    openSet.RemoveAt(0);
                }
            }

            return tileCount;
        }
    }
}