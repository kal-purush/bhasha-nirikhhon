// <copyright file="IQuestion.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Questions.LeetCode.No3.LongestSubstringWithoutRepeatingCharacters
{
    /// <summary>
    /// The LeetCode No.3 question: Longest Substring without Repeating Characters.
    /// </summary>
    public interface IQuestion
    {
        /// <summary>
        /// Gets the length of longest substring.
        /// </summary>
        /// <param name="s">The original string.</param>
        /// <returns>The length of longest substring.</returns>
        int LengthOfLongestSubstring(string s);
    }
}