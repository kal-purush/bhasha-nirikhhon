using System;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Returns the minimum value in a 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values to determine the minimum value of.</param>
        /// <typeparam name="TSource">The type of the elements of source that implements IComparable&lt;T&gt;.</typeparam>
        /// <returns>The minimum value in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">No object in source.</exception>
        public static TSource Min<TSource>(this TSource[,]? source) where TSource : IComparable<TSource>
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (source.Length == 0) throw new InvalidOperationException(nameof(source));
            var min = source[0, 0];
            foreach (var value in source)
                if (value.CompareTo(min) < 0)
                    min = value;

            return min;
        }

        /// <summary>
        ///     Invokes a transform function on each element of a 2d-array and returns the minimum value.
        /// </summary>
        /// <param name="source">A 2d-array of values to determine the minimum value of.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TResult">The type of the value returned by selector that implements IComparable&lt;T&gt;.</typeparam>
        /// <returns>The minimum value in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">No object in source.</exception>
        public static TResult Min<TSource, TResult>(this TSource[,]? source, Func<TSource, TResult>? selector)
            where TResult : IComparable<TResult>
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            if (source.Length == 0) throw new InvalidOperationException(nameof(source));
            var min = selector(source[0, 0]);
            foreach (var value in source)
            {
                var result = selector(value);
                if (result.CompareTo(min) < 0)
                    min = result;
            }

            return min;
        }

        /// <summary>
        ///     Returns the minimum value in a 3d-array.
        /// </summary>
        /// <param name="source">A 3d-array of values to determine the minimum value of.</param>
        /// <typeparam name="TSource">The type of the elements of source that implements IComparable&lt;T&gt;.</typeparam>
        /// <returns>The minimum value in the 3d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">No object in source.</exception>
        public static TSource Min<TSource>(this TSource[,,]? source) where TSource : IComparable<TSource>
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (source.Length == 0) throw new InvalidOperationException(nameof(source));
            var min = source[0, 0, 0];
            foreach (var value in source)
                if (value.CompareTo(min) < 0)
                    min = value;

            return min;
        }

        /// <summary>
        ///     Invokes a transform function on each element of a 3d-array and returns the minimum value.
        /// </summary>
        /// <param name="source">A 3d-array of values to determine the minimum value of.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TResult">The type of the value returned by selector that implements IComparable&lt;T&gt;.</typeparam>
        /// <returns>The minimum value in the 3d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">No object in source.</exception>
        public static TResult Min<TSource, TResult>(this TSource[,,]? source, Func<TSource, TResult>? selector)
            where TResult : IComparable<TResult>
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            if (source.Length == 0) throw new InvalidOperationException(nameof(source));
            var min = selector(source[0, 0, 0]);
            foreach (var value in source)
            {
                var result = selector(value);
                if (result.CompareTo(min) < 0)
                    min = result;
            }

            return min;
        }
    }
}