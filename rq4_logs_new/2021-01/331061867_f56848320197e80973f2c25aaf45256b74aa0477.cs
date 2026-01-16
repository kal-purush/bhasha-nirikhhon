using System;
using System.Collections.Generic;

namespace Mulinq.Linq
{
    public static partial class EnumerableExtension
    {
        /// <summary>
        ///     Returns sequence of values that skipped for each specified count.
        /// </summary>
        /// <param name="source">A sequence of values.</param>
        /// <param name="count">A skip count.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sequence of values that skipped for each specified count.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="ArgumentException">count &lt; 0</exception>
        public static IEnumerable<TSource> SkipEach<TSource>(this IEnumerable<TSource> source, int count)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (count < 0) throw new ArgumentException(nameof(count));
            if (count == 0) return source;

            IEnumerable<TSource> Inner()
            {
                var idx = 0;
                count++;
                foreach (var item in source)
                {
                    if (idx == 0) yield return item;
                    idx++;
                    idx %= count;
                }
            }

            return Inner();
        }
    }
}