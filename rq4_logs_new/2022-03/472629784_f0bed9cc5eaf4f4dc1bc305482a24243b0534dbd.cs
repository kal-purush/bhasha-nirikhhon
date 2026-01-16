// <copyright file="IQuestion.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Questions.LeetCode.No16.ThreeSumClosest
{
    /// <summary>
    /// The LeetCode No.16 question: Three Sum Closest.
    /// </summary>
    public interface IQuestion
    {
        /// <summary>
        /// Gets the sum of triplets which is closest to target.
        /// </summary>
        /// <param name="nums">The number array.</param>
        /// <param name="target">The target.</param>
        /// <returns>The closest sum of triplet.</returns>
        int ThreeSumClosest(int[] nums, int target);
    }
}