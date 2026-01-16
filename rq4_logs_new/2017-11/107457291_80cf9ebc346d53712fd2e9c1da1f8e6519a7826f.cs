using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        public class Point
        {
            public int X { get; set; }
            public int Y { get; set; }

            public Point()
            {
                X = 0;
                Y = 0;
            }

            public Point(int x, int y)
            {
                X = x;
                Y = y;
            }
        }
        public static void SortL(List<Point> pos)
        {
            pos = pos.OrderBy(a => a.X + a.Y).ToList();
        }

        static void Main(string[] args)
        {
            List<Point> pos = new List<Point>();
            pos.Add(new Point(2, 4));
            pos.Add(new Point(2, 3));
            pos.Add(new Point(2, 2));
            pos.Add(new Point(2, 1));
            pos.Add(new Point(2, 0));

            Program.SortL(pos);

            for (int i = 0; i <=pos.Count;i++)
                Console.WriteLine(pos[i].X + pos[i].Y);
        }
    }
}