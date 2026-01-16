using System;
using System.Linq;

namespace SquareSequenceAnalyzer
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 1 && (args[0].Equals("-h") || args[0].Equals("--help")))
            {
                Console.WriteLine("Pass 2 lists of numbers");
                Console.WriteLine("Example: SquareSequenceAnalyzer 1,2,3,4 1,4,9,16");
            }
            else if (args.Length < 1)
            {
                Console.WriteLine("You need to pass 2 lists of numbers!");
                Console.WriteLine("Correct usage: SquareSequenceAnalyzer <number>,<number>... <squared_number>,<squared_number>");
            }
            else
            {
                var baseNumbers = args[0].Split(",").ToDoubles().ToList();
                var compoundedNumbers = args[1].Split(",").ToDoubles().ToList();
                if (baseNumbers.Count() == compoundedNumbers.Count())
                {
                    Console.WriteLine(compoundedNumbers.ArePowerOf(baseNumbers, 2)
                        ? "First list squared is equal to second list!"
                        : "First list not squared is equal to second list!");
                }
                else
                {
                    Console.WriteLine("Lists have different lengths!");
                }
            }
        }
    }
}