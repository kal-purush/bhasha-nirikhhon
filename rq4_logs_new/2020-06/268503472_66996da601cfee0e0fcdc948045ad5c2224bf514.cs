using System;
using System.Collections.Generic;

namespace MinMaxIntConcat
{
    public class StringComparer : IComparer<string>
    {
        public int Compare(string x, string y)
        {
            if (x == null)
            {
                return -1;
            }

            if (y == null)
            {
                return 1;
            }
            var pad_length = x.Length > y.Length ? x.Length : y.Length;
            x = x.PadRight(pad_length, '0');
            y = y.PadRight(pad_length, '0');
            var numberOfIterations = x.Length < y.Length ? x.Length : y.Length;
            for (int i = 0; i < numberOfIterations; ++i)
            {
                if (x[i] > y[i])
                {
                    return 1;
                }

                if (x[i] < y[i])
                {
                    return -1;
                }
            }

            if (x.Length < y.Length)
            {
                return 1;
            }

            if (x.Length > y.Length)
            {
                return -1;
            }

            return 0;
        }

    }
}