using System;

namespace Exercise6
{
    class FizzBuzz
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter the first word: ");
            string line1 = Console.ReadLine();
            Console.WriteLine("Enter the second word: ");
            string line2 = Console.ReadLine();
            Console.Write(line1);
            for (int i = 0; i < 30 - line1.Length - line2.Length; i++)
            {
                Console.Write(".");
            }
            Console.Write(line2);

            Console.ReadKey();
        }
    }
}