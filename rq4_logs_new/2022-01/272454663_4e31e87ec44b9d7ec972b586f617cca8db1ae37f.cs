using System;

namespace SalesConsole
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("****************************************"); 
            Console.WriteLine("* Welcome to the RedDeer Sales Console *");
            Console.WriteLine("****************************************\n"); 
            
            Console.WriteLine("Please choose an option:");
            Console.WriteLine("1) Average cost of each item");
            Console.WriteLine("2) Top 3 months of revenue");
            Console.WriteLine("3) Find duplicates in backup");
            Console.WriteLine("4) Average spend split by male/female");
            Console.WriteLine("5) Total sold per day of week");

            var choice = Console.ReadKey();

            switch (choice)
            {
                //...
            }
        }
    }
}