using System;
using System.Collections.Generic;

namespace Mulinq.Multidimensional
{
    public static partial class EnumerableExtensions
    {
        /// <summary>
        ///     Returns the sequence of the specified column of the 2-dimensional array.
        /// </summary>
        /// <param name="source">A 2-dimensional array.</param>
        /// <param name="index">A row index.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sequence of the specified column of the 2-dimensional array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">index &lt; 0 or column &lt;= index.</exception>
        public static IEnumerable<TSource> Column<TSource>(this TSource[,] source, int index)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            var (row, column) = (source.GetLength(0), source.GetLength(1));
            if (index < 0 || column <= index) throw new ArgumentOutOfRangeException(nameof(index));
            for (var i = 0; i < row; i++) yield return source[i, index];
        }

        /// <summary>
        ///     Returns the sequences of each column of the 2-dimensional array.
        /// </summary>
        /// <param name="source">A 2-dimensional array.</param>
        /// <param name="start">A first-column index.</param>
        /// <param name="count">A columns count.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sequences of each column of the 2-dimensional array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     start &lt; 0 or column &lt;= start or count &lt;= 0 or column &lt; start + count
        /// </exception>
        public static IEnumerable<IEnumerable<TSource>> Columns<TSource>(this TSource[,] source, int start, int count)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            var (row, column) = (source.GetLength(0), source.GetLength(1));
            if (start < 0 || column <= start) throw new ArgumentOutOfRangeException(nameof(start));
            if (count <= 0 || column < start + count) throw new ArgumentOutOfRangeException(nameof(count));

            for (var c = start; c < start + count; c++)
            {
                var ret = new TSource[row];
                for (var i = 0; i < row; i++) ret[i] = source[i, c];
                yield return ret;
            }
        }
    }
}