using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace _02_Min_Method
{
    class Program
    {
        static void Main(string[] args)
        {
            int num1 = int.Parse(Console.ReadLine());
            int num2 = int.Parse(Console.ReadLine());
            int num3 = int.Parse(Console.ReadLine());

            int smallest = GetMin(num1, num2, num3);

            Console.WriteLine(smallest);

        }

        static int GetMin(int num1, int num2, int num3)
        {
            int result = 0;

            if (num1 < num2 && num1 < num3)
            {
                result = num1;
            }
            else if (num2 < num1 && num2 < num3)
            {
                result = num2;
            }
            else
            {
                result = num3;
            }
            return result;
        }
    }
}