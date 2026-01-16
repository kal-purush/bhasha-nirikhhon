using System;
using System.Collections.Generic;

namespace Mulinq.Linq
{
    public static partial class EnumerableExtension
    {
        /// <summary>
        ///     Returns the sequence that is the specified operation is applied between each value of the sequence.
        /// </summary>
        /// <param name="source">A sequence of values.</param>
        /// <param name="func">An operation will be applied between for each values.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sequence that is the specified operation is applied between each value of the sequence.</returns>
        /// <exception cref="ArgumentNullException">source or func is null.</exception>
        /// <code>
        /// var delta = new[] { 1, 2, 3, 4, 5 }.Delta((x, y) => y - x);
        /// delta: { 1, 1, 1, 1 }
        /// </code>
        public static IEnumerable<TSource> Delta<TSource>(this IEnumerable<TSource> source,
            Func<TSource, TSource, TSource> func)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (func == null) throw new ArgumentNullException(nameof(func));

            IEnumerable<TSource> Inner()
            {
                using var e = source.GetEnumerator();
                if (!e.MoveNext()) yield break;
                var prev = e.Current;
                while (e.MoveNext())
                {
                    yield return func(prev, e.Current);
                    prev = e.Current;
                }
            }

            return Inner();
        }
    }
}