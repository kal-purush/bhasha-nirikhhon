using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EucleadianNormUsingParams
{
    class Program
    {
        static double Norm(params double[] numbers)
        {
            double result = 0;

            foreach(double number in numbers)
            {
                result += Math.Pow(number, 2); // number * number
            }
            result = Math.Sqrt(result);

            return result;
        }

        static void Main(string[] args)
        {

            Console.WriteLine(Norm(4,6,3,6));
            Console.Read();
        }
    }
}