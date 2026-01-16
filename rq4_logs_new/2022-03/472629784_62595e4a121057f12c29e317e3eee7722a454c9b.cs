// <copyright file="BaseSortHelper.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Algorithms.Sorting
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The base sort helper.
    /// </summary>
    public static class BaseSortHelper
    {
        /// <summary>
        /// Tries to sorts.
        /// </summary>
        /// <typeparam name="T">The date type of each element in source collection.</typeparam>
        /// <param name="source">The source collection.</param>
        /// <param name="comparator">The comparator which ca</param>
        /// <param name="sortedResult">The sorted result.</param>
        /// <exception cref="ArgumentNullException">Once the source or comparator is null, this exception will be threw,</exception>
        /// <returns>The sorted status, true means the source collection has been sorted.</returns>
        public static bool TrySort<T>(IEnumerable<T> source, Func<T, T, bool> comparator, out IEnumerable<T> sortedResult)
        {
            sortedResult = null;

            if (source == null || !source.Any())
            {
                throw new ArgumentNullException($"The {nameof(source)} is null or empty which is unnecessary to sort.");
            }

            if (source.Count() == 1)
            {
                sortedResult = source;

                return true;
            }

            if (comparator == null)
            {
                throw new ArgumentNullException($"The {nameof(source)} is null which is invalid.");
            }

            if (source.Count() != 2)
            {
                return false;
            }

            var sourceArray = source.ToArray();

            if (comparator(sourceArray.First(), sourceArray.Last()))
            {
                (sourceArray[0], sourceArray[1]) = (sourceArray[1], sourceArray[0]);
            }

            sortedResult = sourceArray;

            return true;
        }
    }
}