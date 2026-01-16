using System;
using System.Collections.Generic;
using System.Linq;

namespace Advent2017.Solutions
{
    class Day15
    {
        public const int A = 16807;
        public const int B = 48271;

        public IEnumerable<long> GetSequence(long x, long factor, int mod = 1)
        {
            while (true)
            {
                x = ((x * factor) % 2147483647);
                if(x % mod == 0)
                    yield return x;
            }
        }

        public int FinalCount(int startA, int startB, int iterations, bool part2 = false)
        {
            var a = GetSequence(startA, A, part2 ? 4 : 1);
            var b = GetSequence(startB, B, part2 ? 8 : 1);

            return a.Zip(b, (seqA, seqB) => (seqA & 0xFFFF) == (seqB & 0xFFFF))
                .Take(iterations)
                .Count(val=> val);
        }
    }
}