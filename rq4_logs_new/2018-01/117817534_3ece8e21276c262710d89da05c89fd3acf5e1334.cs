using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace USCLN
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Please input A: ");
            var inputA = Console.ReadLine();
            Console.WriteLine("Please input B: ");
            var inputB = Console.ReadLine();

            var isValidA = int.TryParse(inputA, out int numA);
            var isValidB = int.TryParse(inputB, out int numB);

            if (isValidA && isValidB)
            {
                var uscln = USCLN(numA, numB);
                Console.WriteLine("Result: " + uscln);
            }
            else
            {
                Console.WriteLine("Data invalid");
            }

            Console.ReadLine();
        }

        static int USCLN(int a, int b)
        {
            a = Math.Abs(a);
            b = Math.Abs(b);
            if (a == 0 || b == 0)
                return a + b;

            while (a != b)
            {
                if (a > b)
                    a = a - b;
                else
                    b = b - a;
            }
            return a;
        }
    }
}