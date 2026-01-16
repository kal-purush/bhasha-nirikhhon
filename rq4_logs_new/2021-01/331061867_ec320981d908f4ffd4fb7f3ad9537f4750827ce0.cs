using System;
using System.Collections.Generic;

namespace Mulinq.Linq
{
    public static partial class EnumerableExtension
    {
        /// <summary>
        ///     Returns an item that has a minimum value in a sequence of items.
        /// </summary>
        /// <param name="source">A sequence of values to determine the minimum value of.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <param name="updateEquals">Whether to update the minimum value when compared values are equal; default is false.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TKey">The type of the result of the selector which needs to implement IComparable&lt;TKey&gt;.</typeparam>
        /// <returns>The item that has the minimum value in the sequence.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static TSource MinBy<TSource, TKey>(this IEnumerable<TSource> source,
            Func<TSource, TKey> selector, bool updateEquals = false) where TKey : IComparable<TKey>
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            using var e = source.GetEnumerator();
            if (!e.MoveNext()) throw new InvalidOperationException();
            var ret = e.Current;
            var min = selector(ret);
            while (e.MoveNext())
            {
                var current = e.Current;
                var value = selector(current);
                if (value.CompareTo(min) > 0) continue;
                if (!updateEquals && value.CompareTo(min) == 0) continue;
                ret = current;
                min = value;
            }

            return ret;
        }
    }
}