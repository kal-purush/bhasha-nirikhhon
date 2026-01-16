using System;

namespace _05._Boolean_Variable
{
    class Program
    {
        static void Main(string[] args)
        {
            string varInput = Console.ReadLine();

            bool toBoolean = Convert.ToBoolean(varInput);

            if (toBoolean)
            {
                Console.WriteLine($"Yes");
            }
            else
            {
                Console.WriteLine("No");
            }
        }
    }
}