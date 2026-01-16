using System.Text;

namespace AocNetLib
{
    public class Day14
    {
        public string Solve(string input, int iterations)
        {
            var poly = ParseInput(input);
            for (int i = 0; i < iterations; i++)
            {
                poly.Iterate();
            }
            return poly.GetCountDiff().ToString();
        }

        private Polymer ParseInput(string input)
        {
            var lines = input.Split(new char[] { '\n' }).Select(x => x.Trim()).ToArray();
            var rules = new Dictionary<string, char>(lines.Skip(2).Where(x=>!string.IsNullOrWhiteSpace(x)).Select(x => new KeyValuePair<string, char>(x.Substring(0, 2), x.Last())));
            return new Polymer(lines.First(), rules);
        }

        class Polymer
        {
            List<char> list;
            Dictionary<string, char> rules;

            public Polymer(string str, Dictionary<string, char> rules)
            {
                list = new List<char>(str);
                this.rules = rules;
            }

            public void Iterate()
            {
                for (int i = 1; i < list.Count; i++)
                {
                    string key = string.Concat(list[i - 1], list[i]);
                    if (rules.TryGetValue(key, out char value))
                    {
                        list.Insert(i, value);
                    }
                    i++;
                }
            }

            public long GetCountDiff()
            {
                var counts = list.GroupBy(x=>x).Select(x=>x.LongCount());
                var max = counts.Max();
                var min = counts.Min();
                return max - min;
            }
        }
    }
}