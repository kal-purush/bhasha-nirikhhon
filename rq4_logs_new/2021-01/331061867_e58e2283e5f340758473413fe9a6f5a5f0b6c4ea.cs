using System;
using System.Collections.Generic;

namespace Mulinq.Linq
{
    public static partial class EnumerableExtension
    {
        /// <summary>
        ///     Gets the first element of a sequence.
        /// </summary>
        /// <param name="source">The IEnumerable&lt;T&gt; to return the first element of.</param>
        /// <param name="result">The first element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source contains an element; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static bool TryGetFirst<TSource>(this IEnumerable<TSource> source, out TSource result)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            using var e = source.GetEnumerator();
            var exist = e.MoveNext();
            result = exist ? e.Current : default;
            return exist;
        }

        /// <summary>
        ///     Gets the first element of a sequence with the specified.
        /// </summary>
        /// <param name="source">The IEnumerable&lt;T&gt; to return an element form.</param>
        /// <param name="result">The first element with specified.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source contains an element with the predicated; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">source or predicate is null.</exception>
        public static bool TryGetFirst<TSource>(this IEnumerable<TSource> source, out TSource result,
            Predicate<TSource> predicate)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            foreach (var current in source)
            {
                if (!predicate(current)) continue;
                result = current;
                return true;
            }

            result = default;
            return false;
        }
    }
}