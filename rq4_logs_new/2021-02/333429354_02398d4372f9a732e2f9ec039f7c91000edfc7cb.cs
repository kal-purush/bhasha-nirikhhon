using System;
using System.Globalization;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Calculator
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Please input first number: ");
            var number1 = Console.ReadLine();

            //CultureInfo temp_culture = Thread.CurrentThread.CurrentCulture;
            //Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            bool parseresult = double.TryParse(number1, out double firstnumber);
            //Thread.CurrentThread.CurrentCulture = temp_culture;
            if (parseresult == false)
            {
                Console.WriteLine("Bad input!");
                return;
            }

            Console.WriteLine("Input second number: ");
            var number2 = Console.ReadLine();
            bool resultparse = double.TryParse(number2, out double secondnumber);
            if (resultparse == false)
            {
                Console.WriteLine("Bad input!");
                return;
            }

            Console.WriteLine("Input arithmetic operation: ");
            var operation = Console.Read();
            if (!(operation == '+' || operation == '-' || operation == '*' || operation == '/' || operation == '%'))
            {
                Console.WriteLine("Bad input!");
                return;
            }
           
            switch (operation)
            {
                case '+':
                    Console.WriteLine($"Result: {firstnumber + secondnumber}");
                    break;
                case '-':
                    Console.WriteLine($"Result: {firstnumber - secondnumber}");
                    break;
                case '*':
                    Console.WriteLine($"Result: {+firstnumber * secondnumber}" );
                    break;
                case '/':
                    Console.WriteLine($"Result: {firstnumber / secondnumber}" );
                    break;
                case '%':
                    Console.WriteLine($"Result: {firstnumber % secondnumber}" );
                    break;
            }
        }
    }
}