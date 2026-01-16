using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Day4
{
    public class Arithmetic
    {
        public void Addition(int a, int b)
        {
            Console.WriteLine(a + b);
        }
        public void Subtraction(int a, int b)
        {
            Console.WriteLine(a - b);
        }

        public void UseDelegateAsParameter(MathDelegate del, int a, int b)
        {
            del(a, b);
        }

        public void Multiply(int a, double b)
        {
            Console.WriteLine(a * b);
        }

        public bool DivisionForEvenNumbers(Func<int, int, bool> d)
        {
            if (d(100, 10))
            {
                return true;
            }
            return false;
        }
    }
}