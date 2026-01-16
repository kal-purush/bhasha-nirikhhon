namespace AocNetLib
{
    public class Day9
    {
        public string Solve(string input)
        {
            var map = ParseInput(input);
            Field[] lowPoints = GetLowPoints(map);
            int sum = lowPoints.Sum(x => x.Height + 1);
            return sum.ToString();
        }

        private Field[] GetLowPoints(Field[,] map)
        {
            return map.Cast<Field>().Where(x => x.IsLowest).ToArray();
        }

        private Field[,] ParseInput(string input)
        {
            var lines = input.Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
            int width = lines[0].Length;
            int height = lines.Length;
            var fields = new Field[width, height];
            for (int i = 0; i < width; i++)
            {
                for (int j = 0; j < height; j++)
                {
                    fields[i, j] = new Field(lines[j][i] - '0', i, j);
                }
            }
            for (int i = 0; i < width; i++)
            {
                for (int j = 0; j < height; j++)
                {
                    if (i > 0) fields[i, j].Adjacent.Add(fields[i - 1, j]);
                    if (i < width - 1) fields[i, j].Adjacent.Add(fields[i + 1, j]);
                    if (j > 0) fields[i, j].Adjacent.Add(fields[i, j - 1]);
                    if (j < height - 1) fields[i, j].Adjacent.Add(fields[i , j + 1]);
                }
            }
            return fields;
        }

        public string Solve2(string input)
        {
            var map = ParseInput(input);
            List<List<Field>> basins = GetBasins(map);
            int[] top3 = basins.Select(x => x.Count).OrderByDescending(x => x).Take(3).ToArray();
            return (top3[0] * top3[1] * top3[2]).ToString();
        }

        private List<List<Field>> GetBasins(Field[,] map)
        {
            /*List<List<Field>> basins = new List<List<Field>>();
            int width = map.GetLength(0);
            int height = map.GetLength(1);
            for (int i = 0; i < width; i++)
            {
                for (int j = 0; j < height; j++)
                {
                    var field = map[i, j];
                    var adjacents = field.Adjacent.Where(x=>x.Height != 9);
                    var basin = basins.FirstOrDefault(x => adjacents.Any(y => x.Contains(y)));
                    if (basin == null)
                    {
                        basin = new List<Field>();
                        basins.Add(basin);
                    }
                    basin.Add(field);
                }
            }
            return basins;*/
            var finder = new BasinFinder(map.Cast<Field>().ToList());
            return finder.Search();
        }

        class BasinFinder
        {
            List<Field> map;

            public BasinFinder(List<Field> map)
            {
                this.map = map;
            }

            public List<List<Field>> Search()
            {
                List<List<Field>> basins = new List<List<Field>>();
                foreach (var field in map)
                {
                    if (field.Height == 9) continue;
                    if (basins.Any(x => x.Contains(field))) continue;
                    var basin = new List<Field>();
                    Discover(basin, field);
                    basins.Add(basin);
                }
                return basins;
            }

            private void Discover(List<Field> basin, Field field)
            {
                basin.Add(field);
                foreach (var adj in field.Adjacent)
                {
                    if(adj.Height == 9) continue;
                    if(basin.Contains(adj)) continue;
                    Discover(basin, adj);
                }
            }
        }

        class Field
        {
            int x, y;

            public readonly int Height;

            public readonly List<Field> Adjacent;

            public bool IsLowest => Adjacent.All(x => x.Height > Height);

            public Field(int height, int x, int y)
            {
                this.x = x;
                this.y = y;
                Height = height;
                Adjacent = new List<Field>();
            }
        }
    }
}