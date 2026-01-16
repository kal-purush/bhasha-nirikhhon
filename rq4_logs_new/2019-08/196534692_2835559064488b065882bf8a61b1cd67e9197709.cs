using System;
using System.Collections;
using System.Collections.Generic;

namespace Generics2
{
    internal class CarCollection<T> where T : IDictionary
    {
        private readonly Dictionary<int, string> _cars = null;

        public int Count => _cars.Count;

        public string this[int key]
        {
            get
            {
                if (_cars.ContainsKey(key))
                    return _cars[key];
                else
                    return string.Format("{0} - no car this year in collection", key);
            }
        }

        public CarCollection()
        {
            _cars = new Dictionary<int, string>();
        }

        public void Add(int key, string value)
        {
            _cars.Add(key, value);
        }

        public void Show()
        {
            foreach (KeyValuePair<int, string> keyValue in _cars)
            {
                Console.WriteLine(keyValue.Key + " - " + keyValue.Value);
            }
        }

        public void Remove()
        {
            _cars.Clear();
        }
    }
}