using System;
using System.Collections.Generic;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Filters an array of values based on a predicate that as a sequence.
        /// </summary>
        /// <param name="source">An array to filter.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>An IEnumerable&lt;T&gt; that contains elements from the input sequence that satisfy the condition.</returns>
        /// <exception cref="ArgumentNullException">source or predicate is null.</exception>
        public static IEnumerable<TSource> Where<TSource>(this TSource[,]? source, Func<TSource, bool>? predicate)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (predicate is null) throw new ArgumentNullException(nameof(predicate));

            IEnumerable<TSource> Inner()
            {
                foreach (var value in source)
                    if (predicate(value))
                        yield return value;
            }

            return Inner();
        }
    }
}