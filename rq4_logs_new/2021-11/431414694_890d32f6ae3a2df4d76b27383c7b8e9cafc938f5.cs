using System;
using System.Linq;

namespace Exercise7
{
    class Program
    {
        static void Main(string[] args)
        {
            string repeatProgram;
            Console.WriteLine("Please enter string:");
            do
            {
                var enteredString = Console.ReadLine();
                var count = enteredString.Count(t => char.IsUpper(t));

                Console.WriteLine(count);
                Console.WriteLine("Press - y - to continue.");

                repeatProgram = Console.ReadLine();

            } while (repeatProgram == "y");
        }
    }
}