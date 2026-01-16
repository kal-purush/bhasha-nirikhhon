using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mod3_lab2
{
    class Program
    {
        static void Main(string[] args)
        {
            int first = 9;
            int second = 0;
            Console.WriteLine("Enter your first number: ");
            first = System.Int32.Parse(Console.ReadLine());
            Console.WriteLine("Enter your second number: ");
            second = System.Int32.Parse(Console.ReadLine());

            int result = Divide(first, second);
            Console.WriteLine($"The result of dividing {first} by {second} is {result}");

        }

        //Method Divide() accepts two intgers and return the result of dividing the first by the second.
        //user may pass zero in the second parameter. (illegal operation) w/ no validation the application will crash.
        //Method Divide() that contain Exception handling to deal with divide by zero exceptions.
        static int Divide(int first, int second)
        {
            int result = 0;
            try
            {
                result = first / second;
            }
            catch (DivideByZeroException ex)
            {
                Console.WriteLine("Cannot divide by zero, please provide a non-zero value of your second value.");
            }
            return result;
        }
    }
}