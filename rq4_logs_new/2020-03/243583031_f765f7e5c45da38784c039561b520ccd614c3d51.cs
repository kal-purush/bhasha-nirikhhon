using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;

namespace GigglyLib.ProcGen
{
    public class RoomGenerator
    {
        Random _rand;
        public RoomGenerator(int seed)
        {
            _rand = new Random(seed);
        }

        public void Generate(bool[,] map)
        {
            int rooms = 0;

            var points = GetPoints(map);
            while (points.Count > 0)
            {
                if (points.Count == 0)
                    break;
                var spawnPoint = points[_rand.Next(0, points.Count)];
                int width = _rand.Next(7, 13);
                int height = _rand.Next(7, 13);
                var created = CreateRoom(spawnPoint.x, spawnPoint.y, width, height, map, points);
                rooms += created ? 1 : 0;
            }
            Console.WriteLine($"RoomGenerator finished with {rooms} rooms generated");
        }

        private bool CreateRoom(int sX, int sY, int width, int height, bool[,] map, List<(int x, int y)> points)
        {
            for (int x = (sX - width) - 3; x <= sX + width + 3; x++)
                for (int y = (sY - height) - 3; y <= sY + height + 3; y++)
                    if (map[x < 0 ? 0 : x > map.GetLength(0) ? map.GetLength(0)-1 : x, y < 0 ? 0 : y >= map.GetLength(1) ? map.GetLength(1) - 1 : y] == false)
                    {
                        RemoveAt(sX, sY, points);
                        return false;
                    }

            for (int x = sX - width; x <= sX + width; x++)
                for (int y = sY - height; y <= sY + height; y++)
                    map[x, y] = false;
            return true;
        }

        //private void RemovePointsInRange(int sX, int sY, int width, int height, List<(int x, int y)> points)
        //{
        //    for (int x = sX - width; x <= sX + width; x++)
        //    {
        //        for (int y = sY - height; y <= sY + height; y++)
        //        {
        //            RemoveAt(x, y, points);
        //        }
        //    }
        //}

        private void RemoveAt(int x, int y, List<(int x, int y)> points)
        {
            for (int i = 0; i < points.Count; i++)
                if (points[i].x == x && points[i].y == y)
                {
                    points.RemoveAt(i);
                    i--;
                }
        }

        private List<(int x, int y)> GetPoints(bool[,] map)
        {
            float width = map.GetLength(0);
            float height = map.GetLength(1);

            var output = new List<(int x, int y)>();
            for (int i = 0; i < 500; i++)
            {
                double a = _rand.NextDouble();
                double b = _rand.NextDouble();
                if (b < a)
                {
                    double swap = a;
                    a = b;
                    b = swap;
                }
                var result = (b * Math.Cos(2 * Math.PI * a / b), b * Math.Sin(2 * Math.PI * a / b));
                int x = (int)((result.Item1 * (width / 2f)) + (width / 2f));
                int y = (int)((result.Item2 * (height / 2f)) + (height / 2f));

                if (GetEmptyInRange(x, y, 2, map) == 0)
                    output.Add((x, y));
            }
            return output;
        }

        private int GetEmptyInRange(int x, int y, int range, bool[,] map)
        {
            int startX = x - range;
            int startY = y - range;
            int endX = x + range;
            int endY = y + range;

            int notWalls = 0;

            // Loop from top left corner of scrop to bottomo right corner
            for (int cX = startX; cX <= endX; cX++)
                for (int cY = startY; cY <= endY; cY++)
                    if (!IsWall(cX, cY, map)) { notWalls++; }
            return notWalls;
        }

        private bool IsWall(int x, int y, bool[,] map)
        {
            if (x < 0 || x >= map.GetLength(0))
                return false;
            if (y < 0 || y >= map.GetLength(1))
                return false;
            return map[x, y];
        }
    }
}