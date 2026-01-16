using System;

namespace Euler_001
{
    class Program
    {
        static void Main(string[] args)
        {
            int result = 0;
            int max_number = 1000;
            for (int i = 1; i < max_number; i++)
            {
                if (i % 3 == 0)
                {
                    result = result + i;
                }
                else if (i % 5 == 0)
                {
                    result = result + i;
                }

            }
            Console.WriteLine(result);
        }
    }
}