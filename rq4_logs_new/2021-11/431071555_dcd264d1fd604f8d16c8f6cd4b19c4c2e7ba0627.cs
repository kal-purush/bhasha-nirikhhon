using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CheckOddEven
{
    class Program
    {
        static void CheckOddEven(int number)
        {
            if (number % 2 == 0)
            {
                Console.WriteLine("Even Number");
            }
            else
            {
                Console.WriteLine("Odd Number");
            }
        }
        static void Main(string[] args)
        {
            CheckOddEven(Int32.Parse(Console.ReadLine()));
            Console.ReadLine();
            Console.WriteLine("bye!");
            
        }
    }
}