using System;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Determines whether all elements of a 2d-array satisfy a condition.
        /// </summary>
        /// <param name="source">A 2d-array that contains the elements to apply the predicate to.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>
        ///     true if every element of the source array passes the test in the specified predicate, or if the sequence is
        ///     empty; otherwise, false.
        /// </returns>
        /// <exception cref="ArgumentNullException">source or predicate is null.</exception>
        public static bool All<TSource>(this TSource[,]? source, Func<TSource, bool>? predicate)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (predicate is null) throw new ArgumentNullException(nameof(predicate));
            foreach (var value in source)
                if (!predicate(value))
                    return false;

            return true;
        }

        /// <summary>
        ///     Determines whether all elements of a 3d-array satisfy a condition.
        /// </summary>
        /// <param name="source">A 3d-array that contains the elements to apply the predicate to.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>
        ///     true if every element of the source array passes the test in the specified predicate, or if the sequence is
        ///     empty; otherwise, false.
        /// </returns>
        /// <exception cref="ArgumentNullException">source or predicate is null.</exception>
        public static bool All<TSource>(this TSource[,,]? source, Func<TSource, bool>? predicate)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (predicate is null) throw new ArgumentNullException(nameof(predicate));
            foreach (var value in source)
                if (!predicate(value))
                    return false;

            return true;
        }
    }
}