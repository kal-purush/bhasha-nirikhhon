using System;

namespace Excercise1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Enter first number, Liva, please: ");
            int number1 = Int32.Parse(Console.ReadLine());
            Console.Write("Enter second number, Liva, please: ");
            int number2 = Int32.Parse(Console.ReadLine());
            bool result = (number1 == 15 || number2 == 15 || (number1 - number2) == 15 || (number2 - number1) == 15 || number1 + number2 == 15);
            
          
            Console.WriteLine(result);

            Console.ReadKey();
        }
    }
}