using System;
using System.Collections;
using System.Collections.Generic;

namespace FibonacciSequence
{
    public class FibonacciSequence : IEnumerable<int>
    {
        private readonly int _numberOfElements;

        public FibonacciSequence(int numberOfElements)
        {
            if (numberOfElements > 47)
            {
                throw new ArgumentException("Maximum Fibonacci number that can be represented as integer is on a 47th place!", nameof(numberOfElements));
            }

            if (numberOfElements < 0)
            {
                throw new ArgumentException("Cannot create sequence of negative number's size!", nameof(numberOfElements));
            }
            _numberOfElements = numberOfElements;
        }
        public IEnumerator<int> GetEnumerator()
        {
            var previousNumber = -1;
            var nextNumber = 1;
            for (var i = 0; i < _numberOfElements; i++)
            {
                var sum = previousNumber + nextNumber;
                previousNumber = nextNumber;
                nextNumber = sum;
                yield return sum;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}