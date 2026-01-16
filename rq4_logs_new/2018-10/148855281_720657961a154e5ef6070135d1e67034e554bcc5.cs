using System.Collections.Generic;

namespace SymbioticTS
{
    internal static class ISetExtensions
    {
        /// <summary>
        /// Adds the range of items to the <see cref="ISet{T}"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="set">The set.</param>
        /// <param name="items">The items.</param>
        public static void AddRange<T>(this ISet<T> set, IEnumerable<T> items)
        {
            foreach (T item in items)
            {
                set.Add(item);
            }
        }
    }
}