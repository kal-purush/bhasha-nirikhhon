using System;
using System.Collections.Generic;

namespace MultiplyList.App
{
    class Program
    {
        /// <summary>
        /// Get the product of all numbers in the list
        /// </summary>
        /// <param name="numbers">List of numbers</param>
        /// <returns>Product of numbers</returns>
        public static int MultiplyList(List<int> numbers)
        {
            int result = 1;
            foreach (int number in numbers)
            {
                result *= number;
            }
            return result;
        }
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
        }
    }
}