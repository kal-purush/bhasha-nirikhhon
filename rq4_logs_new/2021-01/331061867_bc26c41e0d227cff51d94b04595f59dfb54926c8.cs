using System;
using System.Collections.Generic;

namespace Mulinq.Linq
{
    public static partial class EnumerableExtension
    {
        /// <summary>
        ///     Gets the last element of a sequence.
        /// </summary>
        /// <param name="source">The IEnumerable&lt;T&gt; to return the last element of.</param>
        /// <param name="result">The last element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source contains an element; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static bool TryGetLast<TSource>(this IEnumerable<TSource> source, out TSource result)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            var exist = false;
            result = default;
            foreach (var current in source)
            {
                result = current;
                exist = true;
            }

            return exist;
        }

        /// <summary>
        ///     Gets the last element of a sequence with the specified.
        /// </summary>
        /// <param name="source">The IEnumerable&lt;T&gt; to return an element form.</param>
        /// <param name="result">The last element with specified.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source contains an element with the predicated; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">source or predicate is null.</exception>
        public static bool TryGetLast<TSource>(this IEnumerable<TSource> source, out TSource result,
            Predicate<TSource> predicate)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));
            var exist = false;
            result = default;
            foreach (var current in source)
            {
                if (!predicate(current)) continue;
                result = current;
                exist = true;
            }

            return exist;
        }
    }
}