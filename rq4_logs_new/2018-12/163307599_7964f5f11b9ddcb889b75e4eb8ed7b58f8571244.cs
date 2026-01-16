using System;

namespace GetSumBetweenNumbers.App
{
    public class Program
    {
        /// <summary>
        /// Get the sum of all numbers between low and high bounds inclusively (iteratively).
        /// If low bound is greater than high bound, returns 0.
        /// </summary>
        /// <param name="minValue">Low bound</param>
        /// <param name="maxValue">High bound</param>
        /// <returns>Sum of all all numbers</returns>
        public static int GetSumBetweenNumbersIter(int minValue, int maxValue)
        {
            int result = 0;
            for(int i = minValue; i < maxValue + 1; i++)
            {
                result += i;
            }
            return result;
        }

        /// <summary>
        /// Get the sum of all numbers between low and high bounds inclusively (recursively).
        /// If low bound is greater than high bound, returns 0.
        /// </summary>
        /// <param name="minValue">Low bound</param>
        /// <param name="maxValue">High bound</param>
        /// <returns>Sum of all all numbers</returns>
        public static int GetSumBetweenNumbersRecur(int minValue, int maxValue)
        {
            if (minValue > maxValue)
                return 0;
            if (minValue == maxValue)
                return minValue;
            return minValue + GetSumBetweenNumbersRecur(minValue + 1, maxValue);
        }
        static void Main(string[] args)
        {
            var (min, max) = (1, 10);
            Console.WriteLine($"The sum (iterative approach) of all numbers between {min} and {max} is {GetSumBetweenNumbersIter(min, max)}");
            Console.WriteLine($"The sum (recursive approach) of all numbers between {min} and {max} is {GetSumBetweenNumbersIter(min, max)}");
            (min, max) = (10, 1);
            Console.WriteLine($"The sum (iterative approach) of all numbers when {min} is greater than {max} is {GetSumBetweenNumbersIter(min, max)}");
            Console.WriteLine($"The sum (recursive approach) of all numbers when {min} is greater than {max} is {GetSumBetweenNumbersRecur(min, max)}");
        }
    }
}