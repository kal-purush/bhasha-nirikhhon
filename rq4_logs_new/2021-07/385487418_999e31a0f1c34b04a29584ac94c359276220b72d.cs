using System;

namespace NumberSquare
{
    class NumberSquare
    {
        static void Main(string[] args)
        {
            int minimum = 1;
            int maximum = 1;
            do
            {
                Console.WriteLine("Min?");
                minimum = Convert.ToInt32(Console.ReadLine());
                Console.WriteLine("Max?");
                maximum = Convert.ToInt32(Console.ReadLine());

            } while (minimum < 1 || maximum < 1 || minimum > maximum);
            string line = "";
            for (int i = 0; i <= maximum - minimum; i++)
            {
                for (int j = 0; j <= maximum - minimum; j++) Console.Write(minimum + (j + i) % (maximum - minimum + 1));
                Console.WriteLine();
            }

            Console.ReadKey();
        }
    }
}