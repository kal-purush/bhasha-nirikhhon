using System;
using System.Collections.Generic;

namespace Mulinq.Linq
{
    public static partial class EnumerableExtension
    {
        /// <summary>
        ///     Returns the sequence of accumulated values at each step.
        /// </summary>
        /// <param name="source">An IEnumerable&lt;T&gt; to accumulate over.</param>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="func">An accumulator function to be invoked on each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TAccumulate">The type of the accumulator value.</typeparam>
        /// <returns>The sequence of accumulated values at each step.</returns>
        /// <exception cref="ArgumentNullException">source or func is null.</exception>
        /// <code>
        /// var items = new [] { 1, 2, 3, 4, 5 };
        /// var aggregated = items.Aggregate(0, (x, y) => x + y);
        /// var cumulated = items.Cumulate(0, (x, y) => x + y);
        /// aggregated: 15
        /// cumulated: { 0, 1, 3, 6, 10, 15 }
        /// </code>
        public static IEnumerable<TAccumulate> Cumulate<TSource, TAccumulate>(this IEnumerable<TSource> source,
            TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> func)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (func == null) throw new ArgumentNullException(nameof(func));

            IEnumerable<TAccumulate> Inner()
            {
                yield return seed;
                foreach (var item in source) yield return seed = func(seed, item);
            }

            return Inner();
        }

        /// <summary>
        ///     Returns the sequence of accumulated values at each step.
        /// </summary>
        /// <param name="source">An IEnumerable&lt;T&gt; to accumulate over.</param>
        /// <param name="func">An accumulator function to be invoked on each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TAccumulate">The type of the accumulator value.</typeparam>
        /// <returns>The sequence of accumulated values at each step.</returns>
        /// <exception cref="ArgumentNullException">source or func is null.</exception>
        /// <code>
        /// var items = new [] { 1, 2, 3, 4, 5 };
        /// var aggregated = items.Aggregate((x, y) => x + y);
        /// var cumulated = items.Cumulate((x, y) => x + y);
        /// aggregated: 15
        /// cumulated: { 0, 1, 3, 6, 10, 15 }
        /// </code>
        public static IEnumerable<TAccumulate> Cumulate<TSource, TAccumulate>(this IEnumerable<TSource> source,
            Func<TAccumulate, TSource, TAccumulate> func)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (func == null) throw new ArgumentNullException(nameof(func));

            IEnumerable<TAccumulate> Inner()
            {
                TAccumulate seed = default;
                yield return seed;
                foreach (var item in source) yield return seed = func(seed, item);
            }

            return Inner();
        }

        /// <summary>
        ///     Returns the sequence of accumulated values at each step.
        /// </summary>
        /// <param name="source">An IEnumerable&lt;T&gt; to accumulate over.</param>
        /// <param name="func">An accumulator function to be invoked on each element.</param>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <returns>The sequence of accumulated values at each step.</returns>
        /// <exception cref="ArgumentNullException">source or func is null.</exception>
        /// <code>
        /// var items = new [] { 1, 2, 3, 4, 5 };
        /// var aggregated = items.Aggregate((x, y) => x + y);
        /// var cumulated = items.Cumulate((x, y) => x + y);
        /// aggregated: 15
        /// cumulated: { 0, 1, 3, 6, 10, 15 }
        /// </code>
        public static IEnumerable<TSource> Cumulate<TSource>(this IEnumerable<TSource> source,
            Func<TSource, TSource, TSource> func)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (func == null) throw new ArgumentNullException(nameof(func));

            IEnumerable<TSource> Inner()
            {
                TSource seed = default;
                yield return seed;
                foreach (var item in source) yield return seed = func(seed, item);
            }

            return Inner();
        }
    }
}