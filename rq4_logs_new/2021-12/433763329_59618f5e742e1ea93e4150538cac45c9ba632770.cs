using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;

namespace AdventOfCode2021
{
    internal class Day13
    {

        public static int Run()
        {
            List<string> lines = File.ReadAllLines("Data/Day13Input.txt").ToList();

            int xMax = 0;
            int yMax = 0;
            List<Point> dots = new List<Point>();
            List<Tuple<bool, int>> folds = new List<Tuple<bool, int>>();
            foreach (var line in lines)
            {
                if (line.StartsWith("fold"))
                {
                    var bits = line.Split(' ')[2].Split('=');
                    folds.Add(new Tuple<bool, int>(bits[0] == "x", int.Parse(bits[1])));
                }
                else if (line != "")
                {
                    var pts = line.Split(',');
                    dots.Add(new Point(int.Parse(pts[0]), int.Parse(pts[1])));
                }
            }

            xMax = dots.Max(dot => dot.X);
            yMax = dots.Max(dot => dot.Y);

            foreach (var fold in folds)
            {

                for (int i = 0; i < dots.Count; i++)
                {
                    if (fold.Item1 && dots[i].X > fold.Item2)
                    {
                        dots[i] = new Point(fold.Item2 - (dots[i].X - fold.Item2), dots[i].Y);
                    }
                    else if (!fold.Item1 && dots[i].Y > fold.Item2)
                    {
                        dots[i] = new Point(dots[i].X, fold.Item2 - (dots[i].Y - fold.Item2));
                    }
                }

                if (fold.Item1)
                {
                    xMax = xMax - (fold.Item2 + 1);
                }
                else 
                {
                    yMax = yMax - (fold.Item2 + 1);
                }
            }



            for (int i = 0; i <= yMax; i++)
            {
                for (int j = 0; j <= xMax + 19; j++)
                {
                    if (dots.Any(dot => dot.Y == i & dot.X == j))
                    {
                        Console.Write("#");
                    }
                    else
                    {
                        Console.Write(".");
                    }
                }
                Console.Write("\r\n");
            }

            return dots.Distinct().Count();
        }
    }
}