namespace AocNetLib
{
    public class Day8
    {
        public string Solve(string input)
        {
            var x = ParseInput(input);
            return x.Sum(x => x.Count(y => y == 1 || y == 4 || y == 7 || y == 8)).ToString();
        }

        private static List<int[]> ParseInput(string input)
        {
            var lines = input.Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
            var decodedLines = lines.Select(x => DecodeLine(x)).ToList();
            return decodedLines;
        }

        private static int[] DecodeLine(string x)
        {
            var parts = x.Split('|');
            var key = parts[0].Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var code = parts[1].Split(new char[] {' '}, StringSplitOptions.RemoveEmptyEntries);
            var decoder = new Decoder(key);
            var result = decoder.Decode(code);
            return result;
        }

        internal class Decoder
        {
            Dictionary<string, int> translation;
            public Decoder(string[] key)
            {
                translation = new Dictionary<string, int>();
                char[] segCF = key.First(x => x.Length == 2).ToCharArray();
                char[] segACF = key.First(x => x.Length == 3).ToCharArray();
                char[] segBCDF = key.First(x => x.Length == 4).ToCharArray();
                char segA = segACF.First(x => !segCF.Contains(x));
                char[][] d235 = key.Where(x => x.Length == 5).Select(x => x.ToCharArray()).ToArray();
                char[] allOptions = "abcdefg".ToCharArray();
                char[] segBE = allOptions.Where(x => d235.Count(y => y.Contains(x)) == 1).ToArray();
                char segE = segBE.First(x => !segBCDF.Contains(x));
                char segB = segBE.First(x => x != segE);
                char[] segBD = segBCDF.Where(x => !segCF.Contains(x)).ToArray();
                char segD = segBD.First(x => x != segB);
                char[][] d069 = key.Where(x => x.Length == 6).Select(x => x.ToCharArray()).ToArray();
                char[] segCDE = allOptions.Where(x => d069.Count(y => y.Contains(x)) == 2).ToArray();
                char segC = segCDE.First(x => x != segD && x != segE);
                char segF = segCF.First(x => x != segC);
                char segG = allOptions.First(x => x != segA && x != segB && x != segC && x != segD && x != segE && x != segF);
                translation[GenerateKey(segA, segB, segC, segE, segF, segG)] = 0;
                translation[GenerateKey(segC, segF)] = 1;
                translation[GenerateKey(segA, segC, segD, segE, segG)] = 2;
                translation[GenerateKey(segA, segC, segD, segF, segG)] = 3;
                translation[GenerateKey(segB, segC, segD, segF)] = 4;
                translation[GenerateKey(segA, segB, segD, segF, segG)] = 5;
                translation[GenerateKey(segA, segB, segD, segE, segF, segG)] = 6;
                translation[GenerateKey(segA, segC, segF)] = 7;
                translation[GenerateKey(segA, segB, segC, segD, segE, segF, segG)] = 8;
                translation[GenerateKey(segA, segB, segC, segD, segF, segG)] = 9;
            }

            private static string GenerateKey(params char[] segments)
            {
                return string.Concat(segments.OrderBy(x => x));
            }

            internal int[] Decode(string[] code)
            {
                int[] ret = new int[code.Length];
                for (int i = 0; i < code.Length; i++)
                {
                    ret[i] = Decode(code[i]);
                }
                return ret;
            }

            private int Decode(string v)
            {
                v = GenerateKey(v.ToCharArray());
                return translation[v];
            }
        }

        public string Solve2(string input)
        {
            var x = ParseInput(input);
            return x.Sum(x => MargeDigits(x)).ToString();
        }

        private int MargeDigits(int[] x)
        {
            int num = 0;
            for (int i = 0; i < x.Length; i++)
            {
                num *= 10;
                num += x[i];
            }
            return num;
        }
    }
}