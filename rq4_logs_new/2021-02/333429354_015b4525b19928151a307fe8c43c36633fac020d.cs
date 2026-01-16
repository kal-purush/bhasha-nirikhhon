using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TMS.Net07.Homework.Task3.Part2
{
    class Factorial
    {
        public static void Mian_Factorial()
        {
            Console.WriteLine("Input your number:");
            var boolean = int.TryParse(Console.ReadLine(), out int number);
            if (boolean == false)
            {
                Console.WriteLine("Bad input factorial");
                return;
            }
            var factorial = 1;
            for (int i = 1; i <= number; i++)
            {
                factorial *= i;
            }
            Console.WriteLine($"Factorial of {number} is: {factorial}");
            Console.ReadLine();
        }
    }
}