namespace AocNetLib
{
    public class Day20
    {
        const int Reserves = 101;
        int w, h;
        bool[] algo;
        bool[,] img;

        public string Solve(string v, int iterations = 2)
        {
            ParseInput(v);
            Print();
            for (int i = 0; i < iterations; i++)
            {
                EnhanceImage();
                Print();
            }
            return CountMarks().ToString();
        }

        private int CountMarks()
        {
            int cnt = 0;
            for (int x = 0; x < w; x++)
            {
                for (int y = 0; y < h; y++)
                {
                    if (img[x, y]) cnt++;
                }
            }
            return cnt;
        }

        private void ParseInput(string input)
        {
            var lines = input.TrimEnd().Split(new char[] { '\n' }).Select(x => x.Trim()).ToArray();
            var algoStr = lines.First();
            var imgStr = lines.Skip(2).ToArray();
            w = imgStr[0].Length + 2 * Reserves;
            h = imgStr.Length + 2 * Reserves;
            algo = algoStr.Select(x => x == '#').ToArray();
            img = new bool[w, h];
            for (int y = 0; y < imgStr.Length; y++)
            {
                for (int x = 0; x < imgStr[0].Length; x++)
                {
                    img[x + Reserves, y + Reserves] = imgStr[y][x] == '#';
                }
            }
        }

        private void EnhanceImage()
        {
            bool[,] outImg = new bool[w, h];
            for (int x = 1; x < w - 1; x++)
            {
                for (int y = 1; y < h - 1; y++)
                {
                    int value = 0;
                    for (int dy = -1; dy <= 1; dy++)
                    {
                        for (int dx = -1; dx <= 1; dx++)
                        {
                            value = (value << 1) | (img[x + dx, y + dy] ? 1 : 0);
                        }
                    }
                    outImg[x, y] = algo[value];
                }
            }
            bool unfilledValues = img[0, 0] ? algo[511] : algo[0];
            for (int y = 0; y < h; y++)
            {
                outImg[0, y] = unfilledValues;
                outImg[1, y] = unfilledValues;
                outImg[w - 2, y] = unfilledValues;
                outImg[w - 1, y] = unfilledValues;
            }
            for (int x = 2; x < w - 2; x++)
            {
                outImg[x, 0] = unfilledValues;
                outImg[x, 1] = unfilledValues;
                outImg[x, h - 2] = unfilledValues;
                outImg[x, h - 1] = unfilledValues;
            }
            img = outImg;
        }

        private void Print()
        {
            Console.WriteLine();
            for (int y = 0; y < h; y++)
            {
                for (int x = 0; x < w; x++)
                {
                    Console.Write(img[x, y] ? '#' : '.');
                }
                Console.WriteLine();
            }
        }
    }
}