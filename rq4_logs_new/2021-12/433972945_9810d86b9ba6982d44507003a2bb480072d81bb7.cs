using System.Text;

namespace AocNetLib
{
    public class Day13
    {
        public string Solve(string input)
        {
            var origami = ParseInput(input);
            origami.Solve(1);
            origami.Map.Print();
            return origami.Map.Count().ToString();
        }

        public string Solve2(string input)
        {
            var origami = ParseInput(input);
            origami.Solve();
            origami.Map.Print();
            return origami.Map.ToString();
        }

        private Origami ParseInput(string input)
        {
            var lines = input.TrimEnd().Split(new char[] { '\n' }).Select(x => x.Trim()).ToArray();
            var dots = lines.TakeWhile(x => !string.IsNullOrEmpty(x)).Select(x => { var p = x.Split(',').Select(int.Parse).ToArray(); return new Point(p[0], p[1]); }).ToList();
            var folds = lines.Skip(dots.Count + 1).Select(x => { var p = x.Split('='); return new Fold(int.Parse(p[1]), p[0].Last() == 'x'); }).ToList();
            return new Origami(dots, folds);
        }

        class Map
        {
            bool[,] table;
            int width;
            int height;

            public Map(int w, int h)
            {
                table = new bool[w, h];
                width = w;
                height = h;
            }

            public void Mark(int x, int y)
            {
                table[x, y] = true;
            }

            public void Fold(bool isX, int p)
            {
                if (isX)
                {
                    for (int x = 1; p + x < width && p - x >= 0; x++)
                    {
                        for (int y = 0; y < height; y++)
                        {
                            table[p - x, y] |= table[p + x, y];
                        }
                    }
                    width = p;
                }
                else
                {

                    for (int x = 0; x < width; x++)
                    {
                        for (int y = 1; p + y < height && p - y >= 0; y++)
                        {
                            table[x, p - y] |= table[x, p + y];
                        }
                    }
                    height = p;
                }
            }

            public int Count()
            {
                int cnt = 0;
                for (int x = 0; x < width; x++)
                {
                    for (int y = 0; y < height; y++)
                    {
                        if(table[x, y]) cnt++;
                    }
                }
                return cnt;
            }

            public void Print()
            {
                Console.WriteLine();
                for (int y = 0; y < height; y++)
                {
                    for (int x = 0; x < width; x++)
                    {
                        Console.Write(table[x, y] ? '#' : '.');
                    }
                    Console.WriteLine();
                }
            }

            override public string ToString()
            {
                StringBuilder sb = new StringBuilder();
                for (int y = 0; y < height; y++)
                {
                    for (int x = 0; x < width; x++)
                    {
                        sb.Append(table[x, y] ? '#' : '.');
                    }
                    sb.AppendLine();
                }
                return sb.ToString();
            }
        }

        class Point
        {
            public readonly int X, Y;
            public Point(int x, int y)
            {
                X = x;
                Y = y;
            }
        }

        public class Fold
        {
            public readonly int P;
            public readonly bool IsX;
            public Fold(int p, bool isX)
            {
                P = p;
                IsX = isX;
            }
        }

        class Origami
        {
            public readonly Map Map;
            List<Fold> folds;

            public Origami(List<Point> points, List<Fold> folds)
            {
                int w = points.Max(x => x.X)+1;
                int h = points.Max(y => y.Y)+1;
                Map = new Map(w, h);
                foreach (var point in points)
                {
                    Map.Mark(point.X, point.Y);
                }
                this.folds = folds;
            }

            public void Solve(int limit = int.MaxValue)
            {
                for (int i = 0; i < folds.Count && i < limit; i++)
                {
                    var fold = folds[i];
                    Map.Fold(fold.IsX, fold.P);
                    //Map.Print();
                }
            }
        }
    }
}