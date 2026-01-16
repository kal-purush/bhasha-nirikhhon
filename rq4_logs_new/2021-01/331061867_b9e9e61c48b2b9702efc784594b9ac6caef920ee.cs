using System;
using System.Collections.Generic;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Determines whether a 2d-array contains a specified element by using a specified IEqualityComparer&lt;T&gt;.
        /// </summary>
        /// <param name="source">A 2d-array in which to locate a value.</param>
        /// <param name="value">The value to locate in the 2d-array.</param>
        /// <param name="comparer">An equality comparer to compare values.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source 2d-array contains an element that has the specified value; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static bool Contains<TSource>(this TSource[,]? source, TSource value,
            IEqualityComparer<TSource> comparer)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            foreach (var x in source)
                if (comparer.Equals(x, value))
                    return true;

            return false;
        }

        /// <summary>
        ///     Determines whether a 2d-array contains a specified element by using the default equality comparer.
        /// </summary>
        /// <param name="source">A 2d-array in which to locate a value.</param>
        /// <param name="value">The value to locate in the 2d-array.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source 2d-array contains an element that has the specified value; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static bool Contains<TSource>(this TSource[,]? source, TSource value)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            return Contains(source, value, EqualityComparer<TSource>.Default);
        }

        /// <summary>
        ///     Determines whether a 3d-array contains a specified element by using a specified IEqualityComparer&lt;T&gt;.
        /// </summary>
        /// <param name="source">A 3d-array in which to locate a value.</param>
        /// <param name="value">The value to locate in the 2d-array.</param>
        /// <param name="comparer">An equality comparer to compare values.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source 3d-array contains an element that has the specified value; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static bool Contains<TSource>(this TSource[,,]? source, TSource value,
            IEqualityComparer<TSource> comparer)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            foreach (var x in source)
                if (comparer.Equals(x, value))
                    return true;

            return false;
        }

        /// <summary>
        ///     Determines whether a 3d-array contains a specified element by using the default equality comparer.
        /// </summary>
        /// <param name="source">A 3d-array in which to locate a value.</param>
        /// <param name="value">The value to locate in the 2d-array.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source 3d-array contains an element that has the specified value; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static bool Contains<TSource>(this TSource[,,]? source, TSource value)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            return Contains(source, value, EqualityComparer<TSource>.Default);
        }
    }
}