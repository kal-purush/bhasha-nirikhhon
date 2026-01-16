using System;
using System.Collections.Generic;

namespace AndroidClient
{
    class ActivityStateRepository<T>
       where T : Java.Lang.Object
    {
        readonly Random rand = new Random();
        readonly Dictionary<string, T> states = new Dictionary<string, T>();

        public string Add(T state)
        {
            var key = rand.Next().ToString();
            while (states.ContainsKey(key))
            {
                key = rand.Next().ToString();
            }
            states[key] = state;
            return key;
        }

        public T Remove(string key)
        {
            if (states.ContainsKey(key))
            {
                var s = states[key];
                states.Remove(key);
                return s;
            }
            else
            {
                return default(T);
            }
        }
    }
}