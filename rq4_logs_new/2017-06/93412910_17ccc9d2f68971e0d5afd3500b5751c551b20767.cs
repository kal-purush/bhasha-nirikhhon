using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TCObjectStorageClient
{
    static class EnumerableUtility
    {
        public static void ForEach<T>(this IEnumerable<T> enumerable, Action<T> action)
        {
            foreach (var el in enumerable)
            {
                action(el);
            }
        }

        public static string ToStringAllElement<T>(this IEnumerable<T> enumerable, string delimiter, Func<T, string> converter)
        {
            if (enumerable.Count() <= 0)
                return string.Empty;
            return enumerable.Select(converter).Aggregate((s1, s2) => s1 + delimiter + s2);
        }

        public static string ToStringAllElement<T>(this IEnumerable<T> enumerable, Func<T, string> converter)
        {
            return ToStringAllElement(enumerable, ", ", converter);
        }
    }
}