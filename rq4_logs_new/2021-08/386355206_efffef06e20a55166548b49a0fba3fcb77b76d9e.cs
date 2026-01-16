using System;

namespace Benchmark
{
    public class MyParser
    {
        public int TryCathParse(string str)
        {
            try
            {
                return int.Parse(str);
            }
            catch (Exception ex)
            {
                return 0;
            }
        }
        public int TryParse(string str)
        {
            int result;

            if (!int.TryParse(str, out result))
            {
                result = 0;
            }
            return result;
        }

        public int TryParseFixed(string str)
        {
            int result;
            int.TryParse(str, out result);
            return result;
        }
    }
}