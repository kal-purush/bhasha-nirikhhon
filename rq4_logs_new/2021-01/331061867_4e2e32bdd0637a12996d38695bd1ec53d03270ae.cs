using System;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Returns the maximum value in a 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values to determine the maximum value of.</param>
        /// <typeparam name="TSource">The type of the elements of source that implements IComparable&lt;T&gt;.</typeparam>
        /// <returns>The maximum value in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">No object in source.</exception>
        public static TSource Max<TSource>(this TSource[,]? source) where TSource : IComparable<TSource>
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (source.Length == 0) throw new InvalidOperationException(nameof(source));
            var max = source[0, 0];
            foreach (var value in source)
                if (value.CompareTo(max) > 0)
                    max = value;

            return max;
        }

        /// <summary>
        ///     Invokes a transform function on each element of a 2d-array and returns the maximum value.
        /// </summary>
        /// <param name="source">A 2d-array of values to determine the maximum value of.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TResult">The type of the value returned by selector that implements IComparable&lt;T&gt;.</typeparam>
        /// <returns>The maximum value in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">No object in source.</exception>
        public static TResult Max<TSource, TResult>(this TSource[,]? source, Func<TSource, TResult>? selector)
            where TResult : IComparable<TResult>
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            if (source.Length == 0) throw new InvalidOperationException(nameof(source));
            var max = selector(source[0, 0]);
            foreach (var value in source)
            {
                var result = selector(value);
                if (result.CompareTo(max) > 0)
                    max = result;
            }

            return max;
        }

        /// <summary>
        ///     Returns the maximum value in a 3d-array.
        /// </summary>
        /// <param name="source">A 3d-array of values to determine the maximum value of.</param>
        /// <typeparam name="TSource">The type of the elements of source that implements IComparable&lt;T&gt;.</typeparam>
        /// <returns>The maximum value in the 3d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">No object in source.</exception>
        public static TSource Max<TSource>(this TSource[,,]? source) where TSource : IComparable<TSource>
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (source.Length == 0) throw new InvalidOperationException(nameof(source));
            var max = source[0, 0, 0];
            foreach (var value in source)
                if (value.CompareTo(max) > 0)
                    max = value;

            return max;
        }

        /// <summary>
        ///     Invokes a transform function on each element of a 3d-array and returns the maximum value.
        /// </summary>
        /// <param name="source">A 3d-array of values to determine the maximum value of.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TResult">The type of the value returned by selector that implements IComparable&lt;T&gt;.</typeparam>
        /// <returns>The maximum value in the 3d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">No object in source.</exception>
        public static TResult Max<TSource, TResult>(this TSource[,,]? source, Func<TSource, TResult>? selector)
            where TResult : IComparable<TResult>
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            if (source.Length == 0) throw new InvalidOperationException(nameof(source));
            var max = selector(source[0, 0, 0]);
            foreach (var value in source)
            {
                var result = selector(value);
                if (result.CompareTo(max) > 0)
                    max = result;
            }

            return max;
        }
    }
}