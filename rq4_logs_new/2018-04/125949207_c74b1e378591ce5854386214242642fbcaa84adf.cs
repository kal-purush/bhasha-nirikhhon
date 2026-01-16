using System;
using System.Collections.Generic;
using System.Text;

namespace HashTables
{
    /// <summary>
    /// Represents a key/value pair to be held by a bucket in the HashTable class.
    /// </summary>
    /// <typeparam name="KeyT">The type of key for this key/value pair</typeparam>
    /// <typeparam name="ValueT">The type of value for this key/value pair</typeparam>
    internal class BucketNode<KeyT, ValueT>
    {
        public KeyT Key { get; }
        public ValueT Value { get; set; }

        /// <summary>
        /// Constructor for the BucketNode type. Sets the key for this
        /// node and optionally its value.
        /// </summary>
        /// <param name="key">The key for this key/value pair node</param>
        /// <param name="value">The value for this key/value pair node</param>
        public BucketNode(KeyT key, ValueT value = default(ValueT))
        {
            Key = key;
            Value = value;
        }

        /// <summary>
        /// The next value in the bucket's linked list
        /// </summary>
        public BucketNode<KeyT, ValueT> Next { get; internal set; }
    }
}