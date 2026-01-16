using System;
using System.Collections.Generic;
using System.Text;

namespace app
{
    public class Printer
    {

        public async void Options()
        {
            List<string> options = new List<string>();
            options.Add("1: Check if Prime");
            options.Add("2: Check Primes between parameters");

            Console.WriteLine("What would you like to do today?");

            foreach (string element in options)
            {
                Console.WriteLine(element);
            };

            var input = Console.ReadLine();
            try
            {
                int result = Int32.Parse(input);
                switch (result)
                {
                    case 1:
                        Console.Clear();
                        Console.WriteLine("What number would you like to check?");
                        string numberToCheck = Console.ReadLine();
                        int resultOfCheck = Int32.Parse(numberToCheck);
                        Requestmaker.RunCheckIfPrime(resultOfCheck).GetAwaiter().GetResult();
                        break;
                    case 2:
                        Console.Clear();
                        Console.WriteLine("Start number?");
                        string numberStart = Console.ReadLine();
                        int startNumConv = Int32.Parse(numberStart);
                        Console.WriteLine("End number?");
                        string numberEnd = Console.ReadLine();
                        int endNumConv = Int32.Parse(numberEnd);
                        Requestmaker.RunCountPrime(startNumConv, endNumConv).GetAwaiter().GetResult();
                        break;
                    default:
                        Console.WriteLine("Invalid Input, please try again");
                        break;
                }
                Console.WriteLine("Press enter to continue...");
                Console.ReadLine();
                Console.Clear();
                Options();
            }
            catch
            {
                Console.WriteLine("Invalid Input, please try again");
            }
        }
    }
}