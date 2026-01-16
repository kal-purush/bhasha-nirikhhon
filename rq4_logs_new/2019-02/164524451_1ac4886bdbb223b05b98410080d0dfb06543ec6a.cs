using System;
using System.Linq;
using System.Collections.Generic;

namespace whiteboard25
{
    class Program
    {
        static void Main(string[] args)
        {
            List<int> numbers = new List<int> { 1, 2, 3, 4, 5 };
            IEnumerable<int> filtered = numbers.Where(IsEven);
            IEnumerable<int> squared = filtered.Select(Square);
            //Console.WriteLine(SquaredSum(numbers));

            //Console.WriteLine(numbers.Where(IsEven()).Select(Square).Sum());
            foreach (var item in filtered)
            {
                Console.WriteLine(item);
            }

            foreach (var item in squared)
            {
                Console.WriteLine(item);
            }
            Console.WriteLine("total");
            int total = squared.Sum();
            Console.WriteLine(total);
        }
        public static int Square(int number)
        {
            Console.WriteLine("Squared Called");
            return number * number;
        }
        public static bool IsEven(int number)
        {
            Console.WriteLine("IsEven Called");
            return number % 2 == 0;
        }
        public static double SquaredSum(List<int> list)
        {
            int sum = 0;
            foreach (var item in list)
            {
                sum = sum + item * item;
            }
            return sum;
        }
    }
}