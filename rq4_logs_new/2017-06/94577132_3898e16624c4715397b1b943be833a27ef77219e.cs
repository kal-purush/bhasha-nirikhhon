using System;
using System.Numerics;

namespace task_DEV_3
{
    // Class
    public class ConsoleInputHelper
    {
        public BigInteger GetInputNumber()
        {
            bool exitEntering = false;
            BigInteger inputNumber = 0;
            while (!exitEntering)
            {
                Console.WriteLine("Enter integer non-negative number. \n");
                string enteredNumber = Console.ReadLine();
                try
                {
                    inputNumber = BigInteger.Parse(enteredNumber);
                    exitEntering = true;
                }
                catch (FormatException)
                {
                    Console.WriteLine("Sorry, can't understand input number format. Please, try again.");
                }

            }
            return inputNumber;
        }
    }
}