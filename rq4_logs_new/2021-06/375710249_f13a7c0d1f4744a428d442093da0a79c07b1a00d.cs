using System;

namespace Exercise7
{
    class Program
    {
        static void Main(string[] args)
        {
            int i = 0;
            do
            {
                Console.Clear();
                i = 1;
                double number = 0;
                Console.Write("Please enter number:");
                number = Convert.ToDouble(Console.ReadLine());
                Console.WriteLine("Rounded value of {0} is {1}", number, (int)(number + 0.5));
                Console.ReadKey();
            } while (i != 0);
        }
    }

}