//-----------------------------------------------------------------------
// <copyright file="CodeContractExtensions.cs" company="Gundersoft">
//     Copyright (c) Christian Gunderman. All rights reserved.
// </copyright>
// <author>Christian Gunderman</author>
//-----------------------------------------------------------------------

namespace Uniscript
{
    using System;

    /// <summary>
    /// Extension assert methods for maintaining class code contracts.
    /// </summary>
    internal static class CodeContractExtensions
    {
        /// <summary>
        /// Asserts that an value is not null.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown if value is null.
        /// </exception>
        /// <typeparam name="T">The type of the value.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <param name="argName">The name of the argument.</param>
        public static void AssertNotNull<T>(this T value, string argName)
        {
            if (value == null)
            {
                throw new ArgumentNullException(string.Format("{0} cannot be null.", argName));
            }
        }

        /// <summary>
        /// Asserts that a string is not null, empty, or whitespace.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown if value is null, empty, or whitespace.
        /// </exception>
        /// <param name="value">The string to check.</param>
        /// <param name="argName">The argument to check.</param>
        public static void AssertNotNullOrEmptyOrWhitespace(this string value, string argName)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException(string.Format("{0} cannot be null, empty, or whitespace.", argName));
            }
        }
    }
}