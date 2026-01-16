using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CollectionOfHelpers.GeneralExtensions
{
    public static class DictionaryExtensions
    {
        /// <summary>
        /// Add the key-value pair to the dictionary and return true if the key doesn't already exist in the dictionary. Return false if the key already exists.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="Dictionary"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool TryAdd<TKey, TValue>(this Dictionary<TKey, TValue> Dictionary, TKey key, TValue value)
        {
            if (!Dictionary.ContainsKey(key))
            {
                Dictionary.Add(key, value);
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}