using System.Collections.Generic;

namespace Mulinq.Multidimensional
{
    public static class EnumerableExtensions
    {
        /// <summary>
        ///     Returns the sequence as a single dimension.
        /// </summary>
        /// <param name="source">A 2-dimensional array.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sequence as a single dimension.</returns>
        public static IEnumerable<TSource> AsEnumerable<TSource>(this TSource[,] source)
        {
            foreach (var item in source) yield return item;
        }

        /// <summary>
        ///     Returns the sequence as a single dimension.
        /// </summary>
        /// <param name="source">A 3-dimensional array.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sequence as a single dimension.</returns>
        public static IEnumerable<TSource> AsEnumerable<TSource>(this TSource[,,] source)
        {
            foreach (var item in source) yield return item;
        }
    }
}