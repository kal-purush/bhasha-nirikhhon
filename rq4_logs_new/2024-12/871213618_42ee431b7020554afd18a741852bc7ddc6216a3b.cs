using System;

namespace MovieDatabase.Exceptions
{
    /// <summary>
    /// When the regular membership user tries to write their 6th review. The max is 5s
    /// </summary>
    public class MaxReviewReachedException : DatabaseException
    {
        public MaxReviewReachedException(string? message) : base(message)
        {
        }
    }
}