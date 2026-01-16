using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetSort
{
    public static partial class SortableExtensions
    {
        /// <summary>
        /// Returns a sorted enumerable per the specified 'key' in the order specified on the 'SortableAttribute' of the key passed in, or the default (Ascending).
        /// </summary>
        /// <typeparam name="T">The type of data in the enumerable being sorted</typeparam>
        /// <param name="items">The data to sort</param>
        /// <param name="key">The 'key' to sort by. This key must be present as the 'SortKey' of exactly one 'SortableAttribute' on a property of type T</param>
        public static IOrderedEnumerable<T> ThenSortByKey<T>(this IOrderedEnumerable<T> items, string key) where T : class
        {
            return ThenSortByKey(items, key, Constants.DEFAULT_NESTED_PROP_DELIMITER);
        }

        /// <summary>
        /// Returns a sorted enumerable per the specified 'key' in the order specified on the 'SortableAttribute' of the key passed in, or the default (Ascending).
        /// </summary>
        /// <typeparam name="T">The type of data in the enumerable being sorted</typeparam>
        /// <param name="items">The data to sort</param>
        /// <param name="key">The 'key' to sort by. This key must be present as the 'SortKey' of exactly one 'SortableAttribute' on a property of type T</param>
        /// <param name="delimiter">The char used to separate nested properties</param>
        public static IOrderedEnumerable<T> ThenSortByKey<T>(this IOrderedEnumerable<T> items, string key, char delimiter) where T : class
        {
            return ThenSortByKey(items, key, delimiter, null as SortDirection?);
        }

        /// <summary>
        /// Returns a sorted enumerable per the specified 'key' and 'order'.
        /// </summary>
        /// <typeparam name="T">The type of data in the enumerable being sorted</typeparam>
        /// <param name="items">The data to sort</param>
        /// <param name="key">The 'key' to sort by. This key must be present as the 'SortKey' of exactly one 'SortableAttribute' on a property of type T</param>
        /// <param name="direction">The direction to sort (Asc | Desc)</param>
        public static IOrderedEnumerable<T> ThenSortByKey<T>(this IOrderedEnumerable<T> items, string key, SortDirection direction) where T : class
        {
            return ThenSortByKey(items, key, Constants.DEFAULT_NESTED_PROP_DELIMITER, direction);
        }

        /// <summary>
        /// Returns a sorted enumerable per the specified 'key' and 'order'.
        /// </summary>
        /// <typeparam name="T">The type of data in the enumerable being sorted</typeparam>
        /// <param name="items">The data to sort</param>
        /// <param name="key">The 'key' to sort by. This key must be present as the 'SortKey' of exactly one 'SortableAttribute' on a property of type T</param>
        /// <param name="direction">The direction to sort (Asc | Desc)</param>
        /// <param name="delimiter">The char used to separate nested properties</param>
        public static IOrderedEnumerable<T> ThenSortByKey<T>(this IOrderedEnumerable<T> items, string key, char delimiter, SortDirection direction) where T : class
        {
            return ThenSortByKey(items, key, delimiter, direction);
        }

        /// <summary>
        /// Returns a sorted enumerable per the specified 'key' in the specified 'order'.
        /// </summary>
        /// <typeparam name="T">The type of data in the enumerable being sorted</typeparam>
        /// <param name="items">The data to sort</param>
        /// <param name="key">The 'key' to sort by. This key must be present as the 'SortKey' of exactly one 'SortableAttribute' on a property of type T</param>
        /// <param name="direction">The direction to sort (Asc | Desc)</param>
        /// <param name="delimiter">The char used to separate nested properties</param>
        public static IOrderedEnumerable<T> ThenSortByKey<T>(this IOrderedEnumerable<T> items, string key, char delimiter, string direction) where T : class
        {
            SortDirection directionEnum = ParseDirection(direction);
            return ThenSortByKey(items, key, delimiter, directionEnum);
        }

        /// <summary>
        /// Returns a sorted enumerable per the specified 'key' in the specified 'order'.
        /// </summary>
        /// <typeparam name="T">The type of data in the enumerable being sorted</typeparam>
        /// <param name="items">The data to sort</param>
        /// <param name="key">The 'key' to sort by. This key must be present as the 'SortKey' of exactly one 'SortableAttribute' on a property of type T</param>
        /// <param name="direction">The direction to sort (Asc | Desc)</param>
        public static IOrderedEnumerable<T> ThenSortByKey<T>(this IOrderedEnumerable<T> items, string key, string direction) where T : class
        {
            return ThenSortByKey(items, key, Constants.DEFAULT_NESTED_PROP_DELIMITER, direction);
        }

        private static IOrderedEnumerable<T> ThenSortByKey<T>(IOrderedEnumerable<T> items, string key, char delimiter, SortDirection? direction) where T : class
        {
            IEnumerable<SortOperationMetadata> metadata = SortOperationMetadataFinder.Find<T>(key, delimiter, direction);
            if (metadata.Any() == false)
            {
                string error = $"A 'sortable attribute' could not be found.\n Key: {key} \nType: {typeof(T).Name}.";
                throw new ArgumentException(error);
            }

            return Sort(items, metadata);
        }

        private static IOrderedEnumerable<T> ThenSort<T>(IOrderedEnumerable<T> items, IEnumerable<SortOperationMetadata> metadata)
        {
            if (metadata.Last().Direction == SortDirection.Asc)
            {
                return items.ThenBy(i => GetNestedValue(metadata, i));
            }

            return items.ThenByDescending(i => GetNestedValue(metadata, i));
        }
    }
}