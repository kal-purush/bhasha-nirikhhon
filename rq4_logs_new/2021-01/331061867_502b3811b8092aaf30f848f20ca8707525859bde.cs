using System;
using System.Collections.Generic;

namespace Mulinq.Linq
{
    public static partial class EnumerableExtension
    {
        /// <summary>
        ///     Returns sequences that split by specified size.
        /// </summary>
        /// <param name="source">A sequence of values.</param>
        /// <param name="size">Size of each sequence after splitting.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sequences that split by specified size.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="ArgumentException">size &lt;= 0</exception>
        public static IEnumerable<IEnumerable<TSource>> SplitBy<TSource>(this IEnumerable<TSource> source, int size)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (size <= 0) throw new ArgumentException(nameof(size));

            IEnumerable<IEnumerable<TSource>> Inner()
            {
                var idx = 0;
                var ret = new TSource[size];
                foreach (var x in source)
                {
                    ret[idx++] = x;
                    if (idx == size) yield return ret;
                    idx %= size;
                }

                if (idx > 0) yield return ret[..idx];
            }

            return Inner();
        }
    }
}