using System;

namespace Mulinq.Multidimensional
{
    public static partial class ArrayExtension
    {
        /// <summary>
        ///     Computes the average of a 2d-array of Int32 values.
        /// </summary>
        /// <param name="source">A 2d-array of Int32 values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="OverflowException">The average is larger than MaxValue.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static double Average(this Int32[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Int64 sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    sum += v;
                    count++;
                }
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return (double)sum / count;
        }

        /// <summary>
        ///     Computes the average of a 2d-array of nullable Int32 values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Int32 values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="OverflowException">The average is larger than MaxValue.</exception>
        public static double? Average(this Int32?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Int64 sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    if (!v.HasValue) continue;
                    sum += v.Value;
                    count++;
                }
            }

            return count > 0 ? (double?)sum / count : null;
        }

        /// <summary>
        ///     Computes the average of a 2d-array of Int64 values.
        /// </summary>
        /// <param name="source">A 2d-array of Int64 values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="OverflowException">The average is larger than MaxValue.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static double Average(this Int64[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Int64 sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    sum += v;
                    count++;
                }
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return (double)sum / count;
        }

        /// <summary>
        ///     Computes the average of a 2d-array of nullable Int64 values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Int64 values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="OverflowException">The average is larger than MaxValue.</exception>
        public static double? Average(this Int64?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Int64 sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    if (!v.HasValue) continue;
                    sum += v.Value;
                    count++;
                }
            }

            return count > 0 ? (double?)sum / count : null;
        }

        /// <summary>
        ///     Computes the average of a 2d-array of Single values.
        /// </summary>
        /// <param name="source">A 2d-array of Single values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static Single Average(this Single[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Double sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                sum += v;
                count++;
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return (Single)(sum / count);
        }

        /// <summary>
        ///     Computes the average of a 2d-array of nullable Single values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Single values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Single? Average(this Single?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Double sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                if (!v.HasValue) continue;
                sum += v.Value;
                count++;
            }

            return count > 0 ? (Single?)(sum / count) : null;
        }

        /// <summary>
        ///     Computes the average of a 2d-array of Double values.
        /// </summary>
        /// <param name="source">A 2d-array of Double values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static Double Average(this Double[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Double sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                sum += v;
                count++;
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return sum / count;
        }

        /// <summary>
        ///     Computes the average of a 2d-array of nullable Double values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Double values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Double? Average(this Double?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Double sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                if (!v.HasValue) continue;
                sum += v.Value;
                count++;
            }

            return count > 0 ? (Double?)(sum / count) : null;
        }

        /// <summary>
        ///     Computes the average of a 2d-array of Decimal values.
        /// </summary>
        /// <param name="source">A 2d-array of Decimal values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static Decimal Average(this Decimal[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Decimal sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                sum += v;
                count++;
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return sum / count;
        }

        /// <summary>
        ///     Computes the average of a 2d-array of nullable Decimal values.
        /// </summary>
        /// <param name="source">A 2d-array of nullable Decimal values to calculate the average of.</param>
        /// <returns>The average of the values in the 2d-array.</returns>
        /// <exception cref="ArgumentNullException">source is null.</exception>
        public static Decimal? Average(this Decimal?[,]? source)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            Decimal? sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                if (!v.HasValue) continue;
                sum += v.Value;
                count++;
            }

            return count > 0 ? sum / count : null;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of Int32 values that are obtained by invoking a transform function on each
        ///     element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="OverflowException">The average is larger than MaxValue.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static double Average<TSource>(this TSource[,]? source, Func<TSource, Int32>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Int64 sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    sum += selector(v);
                    count++;
                }
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return (double)sum / count;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of nullable Int32 values that are obtained by invoking a transform function on
        ///     each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="OverflowException">The average is larger than MaxValue.</exception>
        public static double? Average<TSource>(this TSource[,]? source, Func<TSource, Int32?>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Int64 sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    var x = selector(v);
                    if (!x.HasValue) continue;
                    sum += x.Value;
                    count++;
                }
            }

            return count > 0 ? (double?)sum / count : null;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of Int64 values that are obtained by invoking a transform function on each
        ///     element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="OverflowException">The average is larger than MaxValue.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static double Average<TSource>(this TSource[,]? source, Func<TSource, Int64>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Int64 sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    sum += selector(v);
                    count++;
                }
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return (double)sum / count;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of nullable Int64 values that are obtained by invoking a transform function on
        ///     each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="OverflowException">The average is larger than MaxValue.</exception>
        public static double? Average<TSource>(this TSource[,]? source, Func<TSource, Int64?>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Int64 sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    var x = selector(v);
                    if (!x.HasValue) continue;
                    sum += x.Value;
                    count++;
                }
            }

            return count > 0 ? (double?)sum / count : null;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of Single values that are obtained by invoking a transform function on each
        ///     element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static Single Average<TSource>(this TSource[,]? source, Func<TSource, Single>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Double sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                sum += selector(v);
                count++;
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return (Single)(sum / count);
        }

        /// <summary>
        ///     Computes the average of the 2d-array of nullable Single values that are obtained by invoking a transform function
        ///     on each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Single? Average<TSource>(this TSource[,]? source, Func<TSource, Single?>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Double sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    var x = selector(v);
                    if (!x.HasValue) continue;
                    sum += x.Value;
                    count++;
                }
            }

            return count > 0 ? (Single?)(sum / count) : null;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of Double values that are obtained by invoking a transform function on each
        ///     element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static Double Average<TSource>(this TSource[,]? source, Func<TSource, Double>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Double sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                sum += selector(v);
                count++;
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return sum / count;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of nullable Double values that are obtained by invoking a transform function
        ///     on each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Double? Average<TSource>(this TSource[,]? source, Func<TSource, Double?>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Double sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    var x = selector(v);
                    if (!x.HasValue) continue;
                    sum += x.Value;
                    count++;
                }
            }

            return count > 0 ? (Double?)(sum / count) : null;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of Decimal values that are obtained by invoking a transform function on each
        ///     element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        /// <exception cref="InvalidOperationException">source contains no elements.</exception>
        public static Decimal Average<TSource>(this TSource[,]? source, Func<TSource, Decimal>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Decimal sum = 0;
            var count = 0;
            foreach (var v in source)
            {
                sum += selector(v);
                count++;
            }

            if (count == 0) throw new InvalidOperationException(nameof(source));
            return sum / count;
        }

        /// <summary>
        ///     Computes the average of the 2d-array of nullable Decimal values that are obtained by invoking a transform function
        ///     on each element of the input 2d-array.
        /// </summary>
        /// <param name="source">A 2d-array of values that are used to calculate a average.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The average of the projected values.</returns>
        /// <exception cref="ArgumentNullException">source or selector is null.</exception>
        public static Decimal? Average<TSource>(this TSource[,]? source, Func<TSource, Decimal?>? selector)
        {
            if (source is null) throw new ArgumentNullException(nameof(source));
            if (selector is null) throw new ArgumentNullException(nameof(selector));
            Decimal sum = 0;
            var count = 0;
            checked
            {
                foreach (var v in source)
                {
                    var x = selector(v);
                    if (!x.HasValue) continue;
                    sum += x.Value;
                    count++;
                }
            }

            return count > 0 ? (Decimal?)(sum / count) : null;
        }
    }
}