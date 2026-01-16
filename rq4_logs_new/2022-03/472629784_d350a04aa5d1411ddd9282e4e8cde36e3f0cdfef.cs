// <copyright file="InsertionSortHelper.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Algorithms.SortingHelpers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The insertion sort helper.
    /// </summary>
    public static class InsertionSortHelper
    {
        /// <summary>
        /// Sorts the elements in source collection by insertion sort algorithm.
        /// </summary>
        /// <typeparam name="T">The data type of each element in source collection.</typeparam>
        /// <param name="source">The source collection.</param>
        /// <param name="comparator">The comparator to determine how to sort.</param>
        /// <returns>The sorted result.</returns>
        public static IEnumerable<T> InsertSort<T>(this IEnumerable<T> source, Func<T, T, bool> comparator)
        {
            if (BaseSortHelper.TrySort(source, comparator, out var sortedResult))
            {
                return sortedResult;
            }

            var sourceArray = source.ToArray();

            for (var unsortedIndex = 1; unsortedIndex < sourceArray.Length; unsortedIndex++)
            {
                var currentUnsortedValue = sourceArray[unsortedIndex];

                for (var sortedIndex = unsortedIndex - 1; sortedIndex >= 0; sortedIndex--)
                {
                    var currentSortedValue = sourceArray[sortedIndex];

                    if (!comparator(currentSortedValue, currentUnsortedValue))
                    {
                        break;
                    }

                    // Since the unsorted value is smaller than sorted value, exchange 2 values, and do next circulation.
                    sourceArray[sortedIndex] = currentUnsortedValue;
                    sourceArray[sortedIndex + 1] = currentSortedValue;
                }
            }

            return sourceArray;
        }
    }
}