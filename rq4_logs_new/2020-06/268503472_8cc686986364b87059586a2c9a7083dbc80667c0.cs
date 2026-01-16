using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;

namespace MinMaxIntConcat
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 1 && (args[0].Equals("-h") || args[0].Equals("--help")))
            {
                Console.WriteLine("Pass any(at least 1) number of integers");
                Console.WriteLine("Example: MinMaxIntConcat 50 9 2 1");
            }
            else if (args.Length < 1)
            {
                Console.WriteLine("You need to pass at least one integer!");
                Console.WriteLine("Correct usage: MinMaxIntConcat <int> <int> ... <int>");
            }
            else
            {
                var list = new List<string>();
                var comparer = new StringComparer();
                for (int i = 0; i < args.Length; i++)
                {
                    if (int.TryParse(args[i], out _))
                    {
                        list.InsertSorted(args[i], comparer.Compare);
                    }
                    else
                    {
                        Console.WriteLine("At least one of the arguments wasn't a number!");
                        return;
                    }
                }

                int? maxValue = null;
                int? minValue = null;
                if (int.TryParse(string.Join("", list), out var value))
                {
                    maxValue = value;
                }

                list.Reverse();
                if (int.TryParse(string.Join("", list), out value))
                {
                    minValue = value;
                }

                if (maxValue.HasValue)
                {
                    Console.WriteLine($"Max -> {maxValue.Value}");
                }
                else
                {
                    Console.WriteLine("Max value doesn't fit into int!");
                }

                if (minValue.HasValue)
                {
                    Console.WriteLine($"Min -> {minValue.Value}");
                }
                else
                {
                    Console.WriteLine("Min value doesn't fit into int!");
                }
            }
        }
    }
}