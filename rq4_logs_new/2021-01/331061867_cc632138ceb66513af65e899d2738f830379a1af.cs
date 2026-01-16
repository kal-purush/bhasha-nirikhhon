using System;
using System.Collections.Generic;
using System.Linq;

namespace Mulinq.Linq
{
    public static partial class EnumerableExtension
    {
        /// <summary>
        ///     Returns the combination sequences from the original sequence. (nCr patterns).
        /// </summary>
        /// <param name="source">A sequence of values.</param>
        /// <param name="count">A number of objects selected.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The combination sequences from the original sequence.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">count &lt;= 0 or source.Count() &gt; count</exception>
        /// <code>
        /// var combination = new []{ 1, 2, 3 }.Combine(2);
        /// combination: { { 1, 2 }, { 1, 3 }, { 2, 3 } }
        /// </code>
        public static IEnumerable<IEnumerable<TSource>> Combine<TSource>(this IEnumerable<TSource> source, int count)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            IEnumerable<IEnumerable<TSource>> Inner()
            {
                var items = source.ToArray();
                if (count <= 0 || items.Length < count) throw new ArgumentOutOfRangeException(nameof(count));
                var idx = 0;
                var ret = new TSource[count];
                foreach (var x in Combination(items.Length, count))
                {
                    ret[idx++] = items[x];
                    if (idx == count) yield return ret;
                    idx %= count;
                }
            }

            return Inner();
        }

        private static IEnumerable<int> Combination(int n, int r)
        {
            var items = new int[r];

            IEnumerable<int> Inner(int step = 0, int value = 0)
            {
                if (step >= r)
                {
                    foreach (var x in items) yield return x;
                    yield break;
                }

                for (var i = value; i <= n - r + step; i++)
                {
                    items[step] = i;
                    foreach (var x in Inner(step + 1, i + 1)) yield return x;
                }
            }

            return Inner();
        }
    }
}