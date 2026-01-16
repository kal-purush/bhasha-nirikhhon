using System.Collections.Generic;
using System.Linq;

namespace GetcuReone.Cdi.Extensions
{
    /// <summary>
    /// Extensions methods for <see cref="IEnumerable{T}"/>
    /// </summary>
    public static class EnumerableExtensions
    {
        /// <summary>
        /// Is null or empty.
        /// </summary>
        /// <typeparam name="TItem"></typeparam>
        /// <param name="items"></param>
        /// <returns></returns>
        public static bool IsNullOrEmpty<TItem>(this IEnumerable<TItem> items)
        {
            return items == null || !items.Any();
        }
    }
}