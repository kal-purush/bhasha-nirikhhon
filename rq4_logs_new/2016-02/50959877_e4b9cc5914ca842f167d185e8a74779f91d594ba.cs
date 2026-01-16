using System.Collections.Generic;

namespace PicnicOrm.Dapper
{
    /// <summary>
    /// </summary>
    public static class Extensions
    {
        #region IDictionary<TKey, T> Methods

        /// <summary>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="items"></param>
        /// <param name="linkDictionary"></param>
        /// <returns></returns>
        public static IDictionary<TKey, IList<T>> ToGrouping<T, TKey>(this IDictionary<TKey, T> items,
            IDictionary<TKey, List<TKey>> linkDictionary)
        {
            IDictionary<TKey, IList<T>> dictionary = new Dictionary<TKey, IList<T>>();

            foreach (var parentLinkKey in linkDictionary.Keys)
            {
                var list = new List<T>();
                foreach (var childLinkKey in linkDictionary[parentLinkKey])
                {
                    if (items.ContainsKey(childLinkKey))
                    {
                        list.Add(items[childLinkKey]);
                    }
                }
                dictionary.Add(parentLinkKey, list);
            }

            return dictionary;
        }

        #endregion
    }
}