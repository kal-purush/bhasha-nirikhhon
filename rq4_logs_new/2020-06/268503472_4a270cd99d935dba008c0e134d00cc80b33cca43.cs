using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;

namespace SquareSequenceAnalyzer
{
    public static class EnumerableExtension
    {
        public static bool ArePowerOf(this IEnumerable<double> compoundedNumbers, IEnumerable<double> baseNumbers, int power)
        {
            return compoundedNumbers.SequenceEqual(baseNumbers.Select(n => Math.Pow(n, power)));
        }

        public static IEnumerable<double> ToDoubles(this IEnumerable<string> stringNumbers)
        {
            return stringNumbers.Select(s =>
            {
                if (double.TryParse(s, out var parsed))
                {
                    return parsed;
                }
                throw new ArgumentException("Couldn't parse all numbers from list!", nameof(stringNumbers));
            });
        }
    }
}