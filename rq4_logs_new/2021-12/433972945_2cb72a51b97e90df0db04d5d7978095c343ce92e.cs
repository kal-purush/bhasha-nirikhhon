using System.Text;

namespace AocNetLib
{
    public class Day15
    {
        public string Solve(string input, int mapSize = 1)
        {
            var map = ParseInput(input, mapSize);
            File.WriteAllText("debug.txt", map.ToString());
            map.FillCosts();
            return map.Last.ToString();
        }

        private Map ParseInput(string input, int mapSize)
        {
            var lines = input.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
            int w = lines[0].Length;
            int h = lines.Length;
            var map = new Map(w * mapSize, h * mapSize);
            for (int x1 = 0; x1 < mapSize; x1++)
            {
                for (int y1 = 0; y1 < mapSize; y1++)
                {
                    for (int x = 0; x < w; x++)
                    {
                        for (int y = 0; y < h; y++)
                        {
                            int v = (lines[y][x] - '0' + x1 + y1 - 1) % 9 + 1;
                            map.Table[x + w * x1, y + h * y1] = v;
                        }
                    }
                }
            }
            return map;
        }

        class Map
        {
            int width, height;

            public readonly int[,] Table;
            public readonly int[,] Costs;

            public int Last => Costs[width - 1, height - 1];

            public Map(int w, int h)
            {
                width = w; height = h;
                Table = new int[w, h];
                Costs = new int[w, h];
            }

            public void FillCosts()
            {
                for (int x = 0; x < width; x++)
                {
                    for (int y = 0; y < height; y++)
                    {
                        Costs[x, y] = int.MaxValue;
                    }
                }
                Costs[0, 0] = 0;
                bool isUpdated;
                do
                {
                    isUpdated = false;
                    for (int x = 0; x < width; x++)
                    {
                        for (int y = 0; y < height; y++)
                        {
                            int cost = FindMin(x, y);
                            if (cost < Costs[x, y])
                            {
                                Costs[x, y] = cost;
                                isUpdated = true;
                            }
                        }
                    }
                }
                while (isUpdated);
            }

            int FindMin(int x, int y)
            {
                int cost = Table[x, y];
                int min = Costs[x, y] - cost;
                if (x > 0 && min > Costs[x - 1, y]) min = Costs[x - 1, y];
                if (y > 0 && min > Costs[x, y - 1]) min = Costs[x, y - 1];
                if (x < width - 2 && min > Costs[x + 1, y]) min = Costs[x + 1, y];
                if (y < height - 2 && min > Costs[x, y + 1]) min = Costs[x, y + 1];
                return min + cost;
            }

            public override string ToString()
            {
                StringBuilder sb = new StringBuilder();
                for (int y = 0; y < height; y++)
                {
                    for (int x = 0; x < width; x++)
                    {
                        sb.Append(Table[x, y]);
                    }
                    sb.AppendLine();
                }
                return sb.ToString();
            }
        }
    }
}