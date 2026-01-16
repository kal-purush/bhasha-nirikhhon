// <copyright file="MergingSortHelper.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Algorithms.SortingHelpers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The merging sort helper.
    /// </summary>
    public static class MergingSortHelper
    {
        /// <summary>
        /// Sorts the elements in source collection by merging algorithm.
        /// </summary>
        /// <typeparam name="T">The data type of each element in source collection.</typeparam>
        /// <param name="source">The source collection.</param>
        /// <param name="comparator">The comparator to determine how to sort.</param>
        /// <returns>The sorted result.</returns>
        public static IEnumerable<T> MergingSort<T>(this IEnumerable<T> source, Func<T, T, bool> comparator)
        {
            return BaseSortHelper.TrySort(source, comparator, out var sortedResult)
                ? sortedResult
                : SortByMerging(source.ToArray(), comparator);
        }

        /// <summary>
        /// Sorts the elements by merging.
        /// </summary>
        /// <typeparam name="T">The data type of each element in source collection.</typeparam>
        /// <param name="sourceArray">The source array.</param>
        /// <param name="comparator">The comparator to determine how to sort.</param>
        /// <returns>The sorted result.</returns>
        private static T[] SortByMerging<T>(T[] sourceArray, Func<T, T, bool> comparator)
        {
            switch (sourceArray.Length)
            {
                case 1:
                    return sourceArray;

                case 2:
                    if (comparator(sourceArray.First(), sourceArray.Last()))
                    {
                        (sourceArray[0], sourceArray[1]) = (sourceArray[1], sourceArray[0]);
                    }

                    return sourceArray;
            }

            var middleIndex = sourceArray.Length / 2;
            var firstPartArray = sourceArray.Take(middleIndex).ToArray();
            var lastPartArray = sourceArray.Skip(middleIndex).ToArray();
            var sortedFirstPartArray = SortByMerging<T>(firstPartArray, comparator);
            var sortedLastPartArray = SortByMerging<T>(lastPartArray, comparator);

            return MergeSortedArrays(sortedFirstPartArray, sortedLastPartArray, comparator);
        }

        /// <summary>
        /// Merges sorted arrays.
        /// </summary>
        /// <typeparam name="T">The data type of each element in source collection.</typeparam>
        /// <param name="sortedFirstPartArray">The first sorted array.</param>
        /// <param name="sortedLastPartArray">The second sorted array.</param>
        /// <param name="comparator">The comparator to determine how to sort.</param>
        /// <returns>The merged sorted result.</returns>
        private static T[] MergeSortedArrays<T>(
            IReadOnlyList<T> sortedFirstPartArray,
            IReadOnlyList<T> sortedLastPartArray,
            Func<T, T, bool> comparator)
        {
            var mergedResult = new List<T>();
            var firstPartIndex = 0;
            var lastPartIndex = 0;

            while (mergedResult.Count < sortedFirstPartArray.Count + sortedLastPartArray.Count)
            {
                if (firstPartIndex == sortedFirstPartArray.Count && lastPartIndex < sortedLastPartArray.Count)
                {
                    mergedResult.AddRange(sortedLastPartArray.Skip(lastPartIndex));

                    break;
                }

                if (lastPartIndex == sortedLastPartArray.Count && firstPartIndex < sortedFirstPartArray.Count)
                {
                    mergedResult.AddRange(sortedFirstPartArray.Skip(firstPartIndex));

                    break;
                }

                if (comparator(sortedFirstPartArray[firstPartIndex], sortedLastPartArray[lastPartIndex]))
                {
                    mergedResult.Add(sortedLastPartArray[lastPartIndex]);
                    lastPartIndex++;
                }
                else
                {
                    mergedResult.Add(sortedFirstPartArray[firstPartIndex]);
                    firstPartIndex++;
                }
            }

            return mergedResult.ToArray();
        }
    }
}