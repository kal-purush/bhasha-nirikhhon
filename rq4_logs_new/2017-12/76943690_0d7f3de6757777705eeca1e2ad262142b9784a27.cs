using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Advent2017.Solutions
{
    class Day10
    {
        public List<int> List = Enumerable.Range(0, 256).ToList();
        private int _currentPosition;
        private int _skipLength;

        public void ApplyInput(int[] input)
        {
            _currentPosition = 0;
            _skipLength = 0;

            //Then, for each length:
            //Reverse the order of that length of elements in the list, starting with the element at the current position.
            //Move the current position forward by that length plus the skip size.
            //Increase the skip size by one.
            //3, 4, 1, 5
            RunRound(input);
        }

        private void RunRound(int[] input)
        {
            foreach (var length in input)
            {
                //start a new list from the current position, wrapping around
                List = List.Skip(_currentPosition).Concat(List.Take(_currentPosition)).ToList();
                //reverse the sublist by teh amount of the input and concat the rest of the lsit.
                List = List.Take(length).Reverse().Concat(List.Skip(length)).ToList();
                //put the list back to the way it was before we meddled with it
                List = List.Skip(List.Count - _currentPosition).Concat(List.Take(List.Count - _currentPosition)).ToList();

                _currentPosition = _currentPosition = (length + _skipLength + _currentPosition) % List.Count;

                _skipLength++;
            }
        }

        public string KnotHash(int[] bytes)
        {
            var input = bytes.Select(x => (int)x).Concat(new[] { 17, 31, 73, 47, 23 }).ToArray();

            _currentPosition = 0;
            _skipLength = 0;

            foreach (var unused in Enumerable.Repeat(0,64))
            {
                RunRound(input);
            }
            
            var denseHash = new List<int>();
            

            for (var x = 0; x < List.Count - 1; x += 16)
            {
                var hash = 0;

                for (var i = 0; i < 16; i++)
                {
                    hash ^= List[x + i];
                }

                denseHash.Add(hash);
            }

            return denseHash.Aggregate(new StringBuilder(),(x,y) => x.Append($"{y:x2}")).ToString();
        }
    }
}