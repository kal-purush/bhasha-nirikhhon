// <copyright file="IQuestion.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Questions.LeetCode.No1.TwoSum
{
    /// <summary>
    /// The LeetCode No.1 question: Two Sum.
    /// </summary>
    public interface IQuestion
    {
        /// <summary>
        /// Gets the indexes of 2 numbers that their sum equals to target.
        /// </summary>
        /// <param name="nums">The number array.</param>
        /// <param name="target">The target.</param>
        /// <returns>The indexes indicate 2 numbers which sum equals to target.</returns>
        int[] TwoSum(int[] nums, int target);
    }
}