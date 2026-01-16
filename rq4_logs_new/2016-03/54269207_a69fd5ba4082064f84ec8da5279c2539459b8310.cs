using FuzzyString;
using System;
using System.Collections.Generic;

namespace TranslationMod
{
    public class FuzzyStringDictionary
    {
        private Dictionary<string, string> _dictionary;
        public FuzzyStringDictionary(Dictionary<string, string> dictioanry)
        {
            _dictionary = dictioanry;
        }

        public string this[string key]
        {
            get
            {
                return _dictionary[CompareKey(key)];
            }
            set
            {
                _dictionary[CompareKey(key)] = value;
            }
        }

        public int Count
        {
            get
            {
                return _dictionary.Count;
            }
        }

        public ICollection<string> Keys
        {
            get
            {
                return _dictionary.Keys;
            }
        }

        public ICollection<string> Values
        {
            get
            {
                return _dictionary.Values;
            }
        }

        public void Add(string key, string value)
        {
            _dictionary.Add(key, value);
        }

        public void Clear()
        {
            _dictionary.Clear();
        }

        public bool ContainsKey(string key)
        {
            var removeItem = CompareKey(key);
            if (string.IsNullOrEmpty(removeItem)) return false;
            else return true;
        }

        public bool Remove(string key)
        {
            var removeItem = CompareKey(key);
            if (string.IsNullOrEmpty(removeItem)) return false;
            return _dictionary.Remove(removeItem);
        }

        private string CompareKey(string source)
        {
            double score = 0;
            string resultString = "";
            foreach (var item in _dictionary)
            {
                var tempScore = Convert.ToDouble((source.LongestCommonSubsequence(item.Key).Length) / Convert.ToDouble(Math.Min(source.Length, item.Key.Length)));
                if (tempScore > 0.85 && tempScore > score)
                {
                    score = tempScore;
                    resultString = item.Key;
                }
            }
            return resultString;
        }
    }
}