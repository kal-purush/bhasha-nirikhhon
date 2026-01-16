// <copyright file="IQuestion.cs" company="Ocean">
// Copyright (c) Ocean. All rights reserved.
// </copyright>

namespace Questions.LeetCode.No9.PalindromeNumber
{
    /// <summary>
    /// The LeetCode No.9 question: Palindrome Number.
    /// </summary>
    public interface IQuestion
    {
        /// <summary>
        /// Checks the number whether it is palindrome.
        /// </summary>
        /// <param name="x">The original number.</param>
        /// <returns>The checked result, true means the number is palindrome.</returns>
        bool IsPalindrome(int x);
    }
}