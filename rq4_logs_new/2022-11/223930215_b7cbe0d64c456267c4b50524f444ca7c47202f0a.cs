using System.Collections.Generic;

namespace Peazy.Core
{
    internal static partial class EnumerableHelpers
    {
        /// <summary>
        /// Create a new Stack from an enumerable
        /// </summary>
        /// <param name="source">Source items</param>
        /// <typeparam name="T">The type of elements in enumerable and stack</typeparam>
        /// <returns>Returns a new stack containing elements from enumerable</returns>
		public static Stack<T> ToStack<T>(IEnumerable<T> source)
        {
            return new Stack<T>(source);
        }
    }
}