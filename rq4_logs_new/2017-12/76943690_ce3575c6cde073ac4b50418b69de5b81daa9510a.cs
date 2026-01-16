
using System;
using System.Linq;

namespace Advent2017.Solutions
{
    class Day4
    {
        public bool IsValid(string input)
        {
            var words = input.Split(' ');
            return words.Length == words.Distinct().Count();
        }

        public bool IsValidAnagram(string input)
        {
            var words = input.Split(' ').ToList();
            
            var sorted = words.Select(x => string.Concat(x.OrderBy(c => c))).ToList();
            return sorted.Distinct().Count() == words.Count;
        }
    }
}