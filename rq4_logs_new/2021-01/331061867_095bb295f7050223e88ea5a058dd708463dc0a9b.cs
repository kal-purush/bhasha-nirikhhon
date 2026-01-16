using System;
using System.Collections.Generic;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Projects each element of a source into a new form as a sequence.
        /// </summary>
        /// <param name="source">A array of values to invoke a transform function on.</param>
        /// <param name="selector">
        ///     A transform function to apply to each source element; the second parameter of the function
        ///     represents the index of the source element.
        /// </param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TResult">The type of the value returned by selector.</typeparam>
        /// <returns>
        ///     An IEnumerable&lt;T&gt; whose elements are the result of invoking the transform function on each element of
        ///     source.
        /// </returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static IEnumerable<TResult> Select<TSource, TResult>(this TSource[,]? source,
            Func<TSource, TResult>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));

            IEnumerable<TResult> Inner()
            {
                foreach (var value in source)
                    yield return selector(value);
            }

            return Inner();
        }
    }
}