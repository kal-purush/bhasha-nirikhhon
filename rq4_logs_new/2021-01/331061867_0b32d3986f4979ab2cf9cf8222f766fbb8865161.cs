using System;
using System.Linq;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Computes the sum of a 2d-array of Int32 values.
        /// </summary>
        /// <param name="source">A 2d-array of Int32 values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="OverflowException">The sum is larger than MaxValue.</exception>
        public static Int32 Sum(this Int32[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            var sum = 0;
            checked
            {
                foreach (var v in source) sum += v;
            }

            return sum;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of nullable Int32 values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Int32 values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="OverflowException">The sum is larger than MaxValue.</exception>
        public static Int32? Sum(this Int32?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Int32? value = 0;
            var hasValue = false;
            checked
            {
                foreach (var x in source)
                {
                    hasValue |= x.HasValue;
                    value += x ?? 0;
                }
            }

            return hasValue ? value : null;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of Int64 values.
        /// </summary>
        /// <param name="source">A 2d-array of Int64 values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="OverflowException">The sum is larger than MaxValue.</exception>
        public static Int64 Sum(this Int64[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Int64 sum = 0;
            checked
            {
                foreach (var v in source) sum += v;
            }

            return sum;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of nullable Int64 values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Int64 values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="OverflowException">The sum is larger than MaxValue.</exception>
        public static Int64? Sum(this Int64?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Int64? value = 0;
            var hasValue = false;
            checked
            {
                foreach (var x in source)
                {
                    hasValue |= x.HasValue;
                    value += x ?? 0;
                }
            }

            return hasValue ? value : null;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of Single values.
        /// </summary>
        /// <param name="source">A 2d-array of Single values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Single Sum(this Single[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Double sum = 0;
            foreach (var v in source) sum += v;
            return (Single)sum;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of nullable Single values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Single values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Single? Sum(this Single?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Double? value = 0;
            var hasValue = false;
            foreach (var x in source)
            {
                hasValue |= x.HasValue;
                value += x ?? 0;
            }

            return hasValue ? (Single?)value : null;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of Double values.
        /// </summary>
        /// <param name="source">A 2d-array of Double values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Double Sum(this Double[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Double sum = 0;
            foreach (var v in source) sum += v;
            return sum;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of nullable Double values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Double values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Double? Sum(this Double?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Double? value = 0;
            var hasValue = false;
            foreach (var x in source)
            {
                hasValue |= x.HasValue;
                value += x ?? 0;
            }

            return hasValue ? value : null;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of Decimal values.
        /// </summary>
        /// <param name="source">A 2d-array of Decimal values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Decimal Sum(this Decimal[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Decimal sum = 0;
            foreach (var v in source) sum += v;
            return sum;
        }

        /// <summary>
        ///     Computes the sum of a 2d-array of nullable Decimal values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Decimal values to calculate the sum of.</param>
        /// <returns>The sum of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Decimal? Sum(this Decimal?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Decimal? value = 0;
            var hasValue = false;
            foreach (var x in source)
            {
                hasValue |= x.HasValue;
                value += x ?? 0;
            }

            return hasValue ? value : null;
        }

        /// <summary>
        ///     Computes the sum of the 2d-array of Int32 values that are obtained by invoking a transform function on each element
        ///     of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="OverflowException">The sum is larger than MaxValue.</exception>
        public static Int32 Sum<TSource>(this TSource[,]? source, Func<TSource, Int32>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of nullable Int32 values that are obtained by invoking a transform function on
        ///     each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="OverflowException">The sum is larger than MaxValue.</exception>
        public static Int32? Sum<TSource>(this TSource[,]? source, Func<TSource, Int32?>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of Int64 values that are obtained by invoking a transform function on each element
        ///     of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="OverflowException">The sum is larger than MaxValue.</exception>
        public static Int64 Sum<TSource>(this TSource[,]? source, Func<TSource, Int64>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of nullable Int64 values that are obtained by invoking a transform function on
        ///     each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="OverflowException">The sum is larger than MaxValue.</exception>
        public static Int64? Sum<TSource>(this TSource[,]? source, Func<TSource, Int64?>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of Single values that are obtained by invoking a transform function on each
        ///     element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Single Sum<TSource>(this TSource[,]? source, Func<TSource, Single>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of nullable Single values that are obtained by invoking a transform function on
        ///     each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Single? Sum<TSource>(this TSource[,]? source, Func<TSource, Single?>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of Double values that are obtained by invoking a transform function on each
        ///     element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Double Sum<TSource>(this TSource[,]? source, Func<TSource, Double>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of nullable Double values that are obtained by invoking a transform function on
        ///     each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Double? Sum<TSource>(this TSource[,]? source, Func<TSource, Double?>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of Decimal values that are obtained by invoking a transform function on each
        ///     element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Decimal Sum<TSource>(this TSource[,]? source, Func<TSource, Decimal>? selector) =>
            source.Select(selector).Sum();

        /// <summary>
        ///     Computes the sum of the 2d-array of nullable Decimal values that are obtained by invoking a transform function on
        ///     each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a sum.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sum of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Decimal? Sum<TSource>(this TSource[,]? source, Func<TSource, Decimal?>? selector) =>
            source.Select(selector).Sum();
    }
}