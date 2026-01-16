using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;

namespace FibonacciSequence
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length == 1 && (args[0].Equals("-h") || args[0].Equals("--help")))
            {
                Console.WriteLine("Pass 1 argument of type uint");
                Console.WriteLine("Example: FibonacciSequence 2");
            }
            else if (args.Length == 0)
            {
                Console.WriteLine("You need to pass an argument!");
                Console.WriteLine("Correct usage: FibonacciSequence <uint>");
            }
            else if (int.TryParse(args[0], out var passedSize))
            {
                if (passedSize < 0)
                {
                    Console.WriteLine("ERROR: Fibonacci sequence size cannot be a negative number!");
                }
                else if (passedSize > 47)
                {
                    Console.WriteLine("ERROR: 47th number in Fibonacci Sequence is the last number that can fit into integer type!");
                }
                else
                {
                    IEnumerable<int> fibonacciSequence = new FibonacciSequence(passedSize);
                    fibonacciSequence.ToList().ForEach(e => Console.Write($"{e} "));
                }
                
            }
            else
            {
                Console.WriteLine("ERROR: Passed argument must be a positive integer!");
            }
            
        }
    }
}