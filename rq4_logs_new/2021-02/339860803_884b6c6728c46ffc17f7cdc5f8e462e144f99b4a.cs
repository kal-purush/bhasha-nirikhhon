using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task_1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to ThermoMetrium!");
            bool minBool, maxBool;
            double min, max;
            do
             {
                 Console.Write("Please enter a minimal temperature in Celsius: ");
                 string minStr = Console.ReadLine();
                 minBool = Double.TryParse(minStr, out min);
                if (minBool == false)
                {
                    Console.WriteLine("Incorrect value of temperature. Try again");
                }
             } while (minBool == false);
            do
            {
                Console.Write("Please enter a maximum temperature in Celsius: ");
                string maxStr = Console.ReadLine();
                maxBool = Double.TryParse(maxStr, out max);
                if (maxBool == false)
                {
                    Console.WriteLine("Incorrect value of temperature. Try again");
                }
            } while (maxBool == false);
            double average = (max + min) / 2;
            Console.WriteLine($"Average temperature of the day = {average}");
            Console.ReadKey();
        }
    }
}