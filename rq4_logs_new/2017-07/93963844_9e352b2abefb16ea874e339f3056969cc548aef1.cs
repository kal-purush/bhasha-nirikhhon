using System;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)

        {
            int num1, num2;
            int res = 0;
            input:
            try
            {
                Console.WriteLine("Enter first number: ");
                num1 = Convert.ToInt32(Convert.ToDouble(Console.ReadLine()));
                Console.WriteLine("Enter second number: ");
                num2 = Convert.ToInt32(Console.ReadLine());

                res = num1 / num2;
            }
            catch (DivideByZeroException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: " + ex.Message);
                Console.ResetColor();

                goto input;
            }
            catch (OverflowException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: " + ex.Message);
                Console.ResetColor();

                goto input;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: " + ex.ToString());
                Console.ResetColor();

                goto input;
            }
            finally
            {
                Console.WriteLine("Result: " + res);
            }
            Console.ReadLine();
        }
    }
}