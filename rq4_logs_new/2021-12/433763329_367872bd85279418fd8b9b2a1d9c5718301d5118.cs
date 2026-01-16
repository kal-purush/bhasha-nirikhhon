using System;
using System.Collections.Generic;
using System.Linq;

namespace AdventOfCode2021
{
    internal class BingoGrid
    {
        List<List<Tuple<int, bool>>> _numbers;

        public BingoGrid(List<List<int>> numbers)
        {
            _numbers = new List<List<Tuple<int, bool>>>();
            foreach (var item in numbers)
            {
                _numbers.Add(item.Select(x => new Tuple<int, bool>(x, false)).ToList());
            }
        }

        public void MarkNumber(int number)
        {
            foreach (var row in _numbers)
            {
                for (int i = 0; i < row.Count; ++i)
                {
                    if (row[i].Item1 == number)
                    {
                        row[i] = new Tuple<int, bool>(number, true);
                    }
                }
            }
        }

        public bool IsComplete()
        {
            foreach (var row in _numbers)
            {
                if (row.All(x => x.Item2 == true))
                {
                    return true;
                }
            }
            for (int i = 0; i < 5; ++i)
            {
                if (_numbers.All(x => x[i].Item2 == true))
                {
                    return true;
                }
            }

            return false;
        }
    
        public int UnmarkedSum()
        {
            int sum = 0;
            foreach (var row in _numbers)
            {
                foreach (var num in row)
                {
                    if (num.Item2 == false)
                    {
                        sum += num.Item1;
                    }
                }
            }
            return sum;
        }
    }
}