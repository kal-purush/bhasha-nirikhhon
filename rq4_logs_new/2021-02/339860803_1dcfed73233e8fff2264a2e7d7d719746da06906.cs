using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task_3
{
    class Program
    {
        static void Main(string[] args)
        {
            bool numBool;
            int num, result;
            Console.WriteLine("Welcome to program Parity!");
            do
            {
                Console.Write("Please enter a whole numeric: ");
                string numStr = Console.ReadLine();
                numBool = int.TryParse(numStr, out num);
                if (numBool == false || num == 0)
                {
                    Console.WriteLine("Incorrected numeric! Try again");
                }
                if (num == 0)
                {
                    Console.WriteLine("0 not a parity and add!");
                }
            } while (numBool == false || num == 0);
            result = num % 2;
            if (result == 1)
            {
                Console.WriteLine($"Entered numeric {num} is odd");
            }
            else
            {
                Console.WriteLine($"Entered numeric {num} is parity");
            }
            Console.ReadKey();
        }
    }
}