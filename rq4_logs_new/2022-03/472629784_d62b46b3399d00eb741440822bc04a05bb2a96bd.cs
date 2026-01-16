// <copyright file="IQuestion.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Questions.LeetCode.No4.MedianOfTwoSortedArrays
{
    using Questions.Interfaces;

    /// <summary>
    /// The LeetCode No.4 question: Median of Two Sorted Arrays.
    /// </summary>
    public interface IQuestion : IBaseQuestion
    {
        /// <summary>
        /// Finds the median of 2 sorted arrays.
        /// </summary>
        /// <param name="nums1">The first sorted array.</param>
        /// <param name="nums2">The second sorted array.</param>
        /// <returns>The median of sorted arrays.</returns>
        double FindMedianSortedArrays(int[] nums1, int[] nums2);
    }
}