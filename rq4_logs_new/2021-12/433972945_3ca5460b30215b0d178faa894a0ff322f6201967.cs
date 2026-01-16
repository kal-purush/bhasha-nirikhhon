namespace AocNetLib
{
    public class Day3
    {
        public string Solve(string input)
        {
            var x = ParseInput(input);
            int len = x[0].Length;
            int[] countsOne = new int[len];
            int count = x.Length;
            for (int i = 0; i < count; i++)
            {
                for (int j = 0; j < len; j++)
                {
                    if(x[i][j] == '1') countsOne[j]++;
                }
            }
            int gamma = 0;
            for (int j = 0; j < len; j++)
            {
                gamma <<= 1;
                if(countsOne[j] * 2 > count) gamma++;
            }
            int eps = ~gamma & ((1 << len) - 1);
            return (gamma * eps).ToString();
        }

        public static string[] ParseInput(string input)
        {
            return input.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Trim()).ToArray();
        }

        public string Solve2(string input)
        {
            var x = ParseInput(input).ToList();
            int result1 = Eliminate(x.ToList(), true);
            int result0 = Eliminate(x.ToList(), false);
            return (result0*result1).ToString();
        }

        public static int Eliminate(List<string> x, bool isOnes)
        {
            int bitIdx = 0;
            while (x.Count > 1)
            {
                int totalCount = x.Count;
                int count = 0;
                for (int i = 0; i < totalCount; i++)
                {
                    if (x[i][bitIdx] == '1') count++;
                }
                char toEliminate;
                toEliminate = ((count * 2 >= totalCount) ^ isOnes) ? '1' : '0';
                x.RemoveAll(x => x[bitIdx] == toEliminate);
                bitIdx++;
            }
            int result = Convert.ToInt32(x[0],2);
            return result;
        }
    }
}