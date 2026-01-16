using System;
using System.Linq;

namespace ArraysKatas
{
    public class FizzBuzz
    {
        /* Return an array containing the numbers from 1 to N, where N is the parametered value.
           Replace certain values however if any of the following conditions are met:
           If the value is a multiple of 3: use the value "Fizz" instead
           If the value is a multiple of 5: use the value "Buzz" instead
           If the value is a multiple of 3 & 5: use the value "FizzBuzz" instead
           N will never be less than 1. 
           Method calling example:
           string[] result = FizzBuzz.GetFizzBuzzArray(3); // => [ "1", "2", "Fizz" ]
           */

        public static string[] GetFizzBuzzArray(int n)
        {
            return Enumerable.Range(1, n).Select(i =>
            {
                if (i % 15 == 0)
                {
                    return "FizzBuzz";
                }

                if (i % 3 == 0)
                {
                    return "Fizz";
                }

                if (i % 5 == 0)
                {
                    return "Buzz";
                }

                return $"{i}"; }).ToArray();
            
            
            //  if (n <= 0) throw new ArgumentOutOfRangeException();
            // .Select(x => x % 15 == 0 ? "FizzBuzz" : x % 3  == 0 ? "Fizz" : x % 5  == 0 ? "Buzz" : x.ToString()).ToArray();
        }
    }
}