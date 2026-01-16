using System;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Determines whether a 2d-array contains any elements.
        /// </summary>
        /// <param name="source">A 2d-array to check for emptiness.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source sequence contains any elements; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static bool Any<TSource>(this TSource[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            var e = source.GetEnumerator();
            return e.MoveNext();
        }

        /// <summary>
        ///     Determines whether any element of a 2d-array satisfies a condition.
        /// </summary>
        /// <param name="source">A 2d-array whose elements to apply the predicate to.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>
        ///     true if the source sequence is not empty and at least one of its elements passes the test in the specified
        ///     predicate; otherwise, false.
        /// </returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static bool Any<TSource>(this TSource[,]? source, Func<TSource, bool>? predicate)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (predicate is null) throw new ArgumentNullException(nameof(predicate));
            foreach (var value in source)
                if (predicate(value))
                    return true;
            return false;
        }

        /// <summary>
        ///     Determines whether a 3d-array contains any elements.
        /// </summary>
        /// <param name="source">A 3d-array to check for emptiness.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>true if the source sequence contains any elements; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static bool Any<TSource>(this TSource[,,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            var e = source.GetEnumerator();
            return e.MoveNext();
        }

        /// <summary>
        ///     Determines whether any element of a 3d-array satisfies a condition.
        /// </summary>
        /// <param name="source">A 3d-array whose elements to apply the predicate to.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>
        ///     true if the source sequence is not empty and at least one of its elements passes the test in the specified
        ///     predicate; otherwise, false.
        /// </returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static bool Any<TSource>(this TSource[,,]? source, Func<TSource, bool>? predicate)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (predicate is null) throw new ArgumentNullException(nameof(predicate));
            foreach (var value in source)
                if (predicate(value))
                    return true;
            return false;
        }
    }
}