using System;

namespace Product1ToN
{
    class Program
    {
        static void Main(string[] args)
        {
            long product = 1;

            const int lowerBound = 1;
            const int upperBound = 10;

            for (int number = lowerBound; number <= upperBound; ++number)
            {
                product *= number;
            }
            
            Console.WriteLine($"The product of all numbers between {lowerBound} and {upperBound} is {product}");
            

            Console.ReadKey();
        }
    }
}