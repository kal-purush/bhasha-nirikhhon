using System;

namespace Exercise8
{
    class Program
    {
        static void Main(string[] args)
        {
            string toRepeat;
            Console.WriteLine("Convert minutes into a numbers of years and days");

            do
            {
                long minutes = Convert.ToInt64(Console.ReadLine());

                double days = minutes / 60 / 24;

                double years = days / 365;

                Console.WriteLine($"Convert {minutes} minutes :" +
                                  $"Days - {days}." +
                                  $"Years - {years}.");

                Console.WriteLine("To continue, press - y -");

                toRepeat = Console.ReadLine();
            } while (toRepeat == "y");
        }
    }
}