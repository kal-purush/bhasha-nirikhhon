using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Advent2017.Solutions
{
    class Day14
    {
        private readonly bool[,] _grid = new bool[128, 128];

        public void BuildGrid(string input)
        {
            Parallel.For(0, 128, x =>
            {
                var hasher = new Day10();
                var hash = hasher.KnotHash($"{input}-{x}");
                //Console.WriteLine(hash);

                var binary = string.Join(String.Empty, hash.Select(c => HexToBinary(c.ToString())));
                //Console.WriteLine(binary);

                for (int i = 0; i < binary.Length; i++)
                {
                    _grid[x, i] = binary[i] == '1';
                }
            });
        }

        public string HexToBinary(string hex)
        {
            //return Convert.ToString(Convert.ToInt32(hex, 16), 2).PadLeft(4, '0');
            StringBuilder sb = new StringBuilder();

            foreach (var c in hex.ToCharArray())
            {
                var intValue = int.Parse(c.ToString(), System.Globalization.NumberStyles.HexNumber);
                sb.Append(Convert.ToString(intValue, 2).PadLeft(4, '0'));
            }

            return sb.ToString();
        }

        public int UsedSquares()
        {
            return _grid.Cast<bool>().Count(b => b);
        }

        public int CountRegions()
        {
            //DFS on first matching used block
            var visited = new bool[128, 128];
            int regions = 0;

            for (int y = 0; y < visited.GetLength(1); y++) // rows
            {
                for (int x = 0; x < visited.GetLength(0); x++) // columns
                {
                    if (visited[x, y] || _grid[x, y] == false)
                    {
                        continue;
                    }

                    Explore(x, y, visited);
                    regions++;
                }
            }
            return regions;
        }

        public void Explore(int x, int y, bool[,] explored)
        {
            if (explored[x, y]) return;
            explored[x, y] = true;
            if (_grid[x, y] == false) return;

            if (x > 0) Explore(x - 1, y, explored);
            if (x < 127) Explore(x + 1, y, explored);
            if (y > 0) Explore(x, y - 1, explored);
            if (y < 127) Explore(x, y + 1, explored);
        }
        

    }
}