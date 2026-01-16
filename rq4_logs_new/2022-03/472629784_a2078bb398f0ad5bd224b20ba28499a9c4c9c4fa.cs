// <copyright file="QuickSortHelper.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Algorithms.SortingHelpers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The quick sort helper.
    /// </summary>
    public static class QuickSortHelper
    {
        /// <summary>
        /// Sorts the elements in source collection by quick sort algorithm.
        /// </summary>
        /// <typeparam name="T">The data type of each element in source array.</typeparam>
        /// <param name="source">The source collection.</param>
        /// <param name="comparator">The comparator to determine how to sort.</param>
        /// <returns>The sorted result.</returns>
        public static IEnumerable<T> QuickSort<T>(this IEnumerable<T> source, Func<T, T, bool> comparator)
        {
            if (BaseSortHelper.TrySort(source, comparator, out var sortedResult))
            {
                return sortedResult;
            }

            var sourceArray = source.ToArray();
            SortByPartition(sourceArray, 0, sourceArray.Length - 1, comparator);

            return sourceArray;
        }

        /// <summary>
        /// Sorts the array by partition.
        /// </summary>
        /// <typeparam name="T">The data type of each element in source array.</typeparam>
        /// <param name="sourceArray">The source array.</param>
        /// <param name="startIndex">The start index.</param>
        /// <param name="endIndex">The end index.</param>
        /// <param name="comparator">The comparator to determine how to sort.</param>
        private static void SortByPartition<T>(IList<T> sourceArray, int startIndex, int endIndex, Func<T, T, bool> comparator)
        {
            while (startIndex < endIndex)
            {
                var partitionIndex = GetPartitionIndex(sourceArray, startIndex, endIndex, comparator);
                SortByPartition(sourceArray, startIndex, partitionIndex - 1, comparator);
                startIndex = partitionIndex + 1;
            }
        }

        /// <summary>
        /// Gets the partition index.
        /// </summary>
        /// <typeparam name="T">The data type of each element in source array.</typeparam>
        /// <param name="sourceArray">The source array.</param>
        /// <param name="startIndex">The start index.</param>
        /// <param name="endIndex">The end index.</param>
        /// <param name="comparator">The comparator to determine how to sort.</param>
        /// <returns>The partition index.</returns>
        private static int GetPartitionIndex<T>(
            IList<T> sourceArray,
            int startIndex,
            int endIndex,
            Func<T, T, bool> comparator)
        {
            var pivot = sourceArray[endIndex];
            var partitionIndex = startIndex;
            var loopIndex = startIndex;

            for (; loopIndex < endIndex; loopIndex++)
            {
                if (!comparator(pivot, sourceArray[loopIndex]))
                {
                    continue;
                }

                (sourceArray[partitionIndex], sourceArray[loopIndex]) =
                    (sourceArray[loopIndex], sourceArray[partitionIndex]);
                partitionIndex++;
            }

            (sourceArray[partitionIndex], sourceArray[endIndex]) = (sourceArray[endIndex], sourceArray[partitionIndex]);

            return partitionIndex;
        }
    }
}