using System;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Returns a number that represents how many elements in the specified 2d-array satisfy a condition.
        /// </summary>
        /// <param name="source">A 2d-array that contains elements to be tested and counted.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>A number that represents how many elements in the 2d-array satisfy the condition in the predicate function.</returns>
        /// <exception cref="ArgumentNullException">source or predicate is null.</exception>
        /// <exception cref="OverflowException">int.MaxValue &lt; count.</exception>
        public static int Count<TSource>(this TSource[,]? source, Func<TSource, bool>? predicate)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (predicate is null) throw new ArgumentNullException(nameof(predicate));
            var count = 0;
            checked
            {
                foreach (var value in source)
                    if (predicate(value))
                        count++;
            }

            return count;
        }

        /// <summary>
        ///     Returns a number that represents how many elements in the specified 3d-array satisfy a condition.
        /// </summary>
        /// <param name="source">A 3d-array that contains elements to be tested and counted.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>A number that represents how many elements in the 3d-array satisfy the condition in the predicate function.</returns>
        /// <exception cref="ArgumentNullException">source or predicate is null.</exception>
        /// <exception cref="OverflowException">int.MaxValue &lt; count.</exception>
        public static int Count<TSource>(this TSource[,,]? source, Func<TSource, bool>? predicate)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (predicate is null) throw new ArgumentNullException(nameof(predicate));
            var count = 0;
            checked
            {
                foreach (var value in source)
                    if (predicate(value))
                        count++;
            }

            return count;
        }
    }
}