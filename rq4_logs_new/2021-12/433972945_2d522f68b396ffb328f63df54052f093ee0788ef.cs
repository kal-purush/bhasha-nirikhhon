namespace AocNetLib
{
    public class Day5
    {
        public string Solve(string input)
        {
            var lines = ParseInput(input).Where(x=>x.IsStraight).ToArray();
            int max = lines.Max(x => x.Max);
            var map = new Map(max+1);
            map.DrawLines(lines);
            //foreach (var line in lines)
            //{
            //    Console.WriteLine(line);
            //}
            //map.PlotMap();
            return map.GetDangerous().ToString();
        }
        static Line[] ParseInput(string input)
        {
            var lines = input.Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
            Line[] ret = lines.Select(ParseLine).ToArray();
            return ret;
        }

        private static Line ParseLine(string line)
        {
            var coords = line.Split(new string[] { ",", "->" }, StringSplitOptions.None).Select(x => int.Parse(x.Trim())).ToArray();
            return new Line(coords[0], coords[1], coords[2], coords[3]);
        }

        class Line
        {
            public int X1;
            public int Y1;
            public int X2;
            public int Y2;

            public bool IsStraight { get => (X1 == X2) || (Y1 == Y2); }
            public int Max { get => new int[]{ X1, X2, Y1, Y2}.Max(); }

            public Line(int x1, int y1, int x2, int y2)
            {
                X1 = x1;
                Y1 = y1;
                X2 = x2;
                Y2 = y2;
            }

            public override string ToString()
            {
                return $"{X1},{Y1} -> {X2},{Y2}"; 
            }
        }

        class Map
        {
            readonly int[,] map;
            readonly int mapsize;

            public Map(int size)
            {
                mapsize = size;
                map = new int[size, size];
            }

            public void DrawLine(Line line)
            {
                int x = line.X1;
                int y = line.Y1;
                int xStep = line.X2 > x ? 1 : -1;
                int yStep = line.Y2 > y ? 1 : -1;

                DrawPoint(x, y);
                while (x != line.X2)
                {
                    x += xStep;
                    DrawPoint(x, y);
                }
                while (y != line.Y2)
                {
                    y += yStep;
                    DrawPoint(x, y);
                }
            }

            public void DrawLines(Line[] lines)
            {
                foreach (var line in lines)
                {
                    DrawLine(line);
                }
            }

            void DrawPoint(int x, int y)
            {
                map[x, y]++;
            }

            public int GetDangerous()
            {
                int count = 0;
                for (int x = 0; x < mapsize; x++)
                {
                    for (int y = 0; y < mapsize; y++)
                    {
                        if (map[x, y] > 1) count++;
                    }
                }
                return count;
            }

            public void PlotMap()
            {
                for (int y = 0; y < mapsize; y++)
                {
                    for (int x = 0; x < mapsize; x++)
                    {
                        int n = map[x, y];
                        char c = n == 0 ? '.' : (n > 9 ? '+' : (char)(n + '0'));
                        Console.Write(map[x, y]);
                    }
                    Console.WriteLine();
                }
            }
        }

    }
}