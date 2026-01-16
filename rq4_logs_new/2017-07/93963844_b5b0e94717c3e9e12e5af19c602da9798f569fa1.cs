using System;

namespace ConsoleApplication1
{
    class CustomExceptionDemo
    {
        public static void Main()
        { 
            Calculator calc = new Calculator();
            int num = -12;
            int res = 0;
            try
            {
                calc.Factorial(num);
            }
            catch (SignedNumberException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: " + ex.Message);
                Console.ResetColor();
            }
            Console.WriteLine("Factorial of {0} is: {1}", num, res);

            Console.ReadLine();
        }
    }

    class SignedNumberException : ApplicationException
    {
        public SignedNumberException(string errorMessage) : base(errorMessage) { }
    }

    class Calculator
    {
        public int Factorial(int num)
        {
            int fact = 1;

            if (num < 0)
            {
                throw new SignedNumberException("Number cannot be -ve.");
            }

            for (int i = 1 ; i <= num; i++)
            {
                fact *= i;
            }
            return fact;
        }
    }

}