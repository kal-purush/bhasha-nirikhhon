using System.Collections.Generic;

namespace Advent2017.Solutions
{
    class Day17
    {
        private readonly LinkedList<int> _buffer = new LinkedList<int>();

        public int Part1(int steps, int repetitions)
        {
            var currentNode = RunSpinLock(steps, repetitions);

            return (currentNode.Next ?? _buffer.First).Value;
        }
        
        public string ReturnBufffer(int steps, int repetitions)
        {
            RunSpinLock(steps, repetitions);
            return string.Join(",", _buffer);
        }

        private LinkedListNode<int> RunSpinLock(int steps, int repetitions)
        {
            _buffer.AddFirst(0);

            var currentNode = _buffer.First;
            for (var i = 1; i <= repetitions; i++)
            {
                for (var j = 0; j < steps; j++)
                {
                    currentNode = currentNode.Next ?? _buffer.First;
                }

                currentNode = _buffer.AddAfter(currentNode, i);
            }
            return currentNode;
        }

        //Brute force isn't quick enough, need to work out how much we've expanded the array by 
        //and keep track of only to right of 0 rather than keeping 50 mil in memory
        public int Part2(int input, int total)
        {
            var index = 0;
            var result = 0;
            for (var i = 1; i < total; i++)
            {
                index = (index + input) % i + 1;
                if (index != 1) continue;
                result = i;
            }
            return result;
        }
    }
}