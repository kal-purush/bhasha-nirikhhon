namespace AocNetLib
{
    public class Day11
    {
        public string Solve(string input, int iterations)
        {
            var map = ParseInput(input);
            int sum = 0;
            for (int i = 0; i < iterations; i++)
            {
                int daily = map.Step();
                sum += daily;
            }
            return sum.ToString();
        }

        public string Solve2(string input)
        {
            var map = ParseInput(input);
            int day = 0;
            int daily;
            do
            {
                daily = map.Step();
                day++;
            } while(daily < 100);
            return day.ToString();
        }

        private Map ParseInput(string input)
        {
            var lines = input.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
            Field[,] map = new Field[10,10];
            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < lines[i].Length; j++)
                {
                    map[i, j] = new Field(lines[i][j]-'0');
                }
            }
            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < lines[i].Length; j++)
                {
                    if (i > 0) map[i, j].Adjacent.Add(map[i - 1, j]);
                    if (i < 9) map[i, j].Adjacent.Add(map[i + 1, j]);
                    if (j > 0) map[i, j].Adjacent.Add(map[i, j - 1]);
                    if (j < 9) map[i, j].Adjacent.Add(map[i, j + 1]);
                    if (i > 0 && j > 0) map[i, j].Adjacent.Add(map[i - 1, j - 1]);
                    if (i < 9 && j > 0) map[i, j].Adjacent.Add(map[i + 1, j - 1]);
                    if (i > 0 && j < 9) map[i, j].Adjacent.Add(map[i - 1, j + 1]);
                    if (i < 9 && j < 9) map[i, j].Adjacent.Add(map[i + 1, j + 1]);
                }
            }
            return new Map(map.Cast<Field>().ToList());
        }

        class Map
        {
            List<Field> map;

            public Map(List<Field> input)
            {
                map = input;
            }

            public int Step()
            {
                map.ForEach(field => field.Increment());
                int count = map.Count(x => x.IsFlashed);
                map.ForEach((field) => field.ClearFlash());
                return count;
            }
        }

        class Field
        {
            public int Value { get; private set; }
            public List<Field> Adjacent { get; private set; }
            public bool IsFlashed { get; private set; }

            public Field(int value)
            {
                Value = value;
                Adjacent = new List<Field>();
            }

            public void Increment()
            {
                Value++;
                if (!IsFlashed && Value > 9)
                {
                    IsFlashed = true;
                    Adjacent.ForEach(x=>x.Increment());
                }
            }

            public void ClearFlash()
            {
                if (IsFlashed)
                {
                    Value = 0;
                    IsFlashed = false;
                }
            }
        }
    }
}