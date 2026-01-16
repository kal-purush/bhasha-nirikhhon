// <copyright file="SelectionSortHelper.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Algorithms.SortingHelpers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The selection sort helper.
    /// </summary>
    public static class SelectionSortHelper
    {
        /// <summary>
        /// Sorts the elements in source collection by selection sort algorithm.
        /// </summary>
        /// <typeparam name="T">The data type of each element in source collection.</typeparam>
        /// <param name="source">The source collection.</param>
        /// <param name="comparator">The comparator to determine how to sort.</param>
        /// <returns>The sorted result.</returns>
        public static IEnumerable<T> SelectionSort<T>(this IEnumerable<T> source, Func<T, T, bool> comparator)
        {
            if (BaseSortHelper.TrySort(source, comparator, out var sortedResult))
            {
                return sortedResult;
            }

            var sourceArray = source.ToArray();

            for (var index = 0; index < sourceArray.Length; index++)
            {
                var minValueIndex = index;

                for (var unsortedIndex = index + 1; unsortedIndex < sourceArray.Length; unsortedIndex++)
                {
                    if (!comparator(sourceArray[minValueIndex], sourceArray[unsortedIndex]))
                    {
                        continue;
                    }

                    minValueIndex = unsortedIndex;
                }

                if (minValueIndex != index)
                {
                    (sourceArray[index], sourceArray[minValueIndex]) = (sourceArray[minValueIndex], sourceArray[index]);
                }
            }

            return sourceArray;
        }
    }
}