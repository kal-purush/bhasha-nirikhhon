using System;
namespace ConsoleApp5
{
    class Program
    {
        static void Main(string[] args)
        {
            Random o1 = new Random();
            int attendance = o1.Next(0, 2);
            if (attendance == 0)
            {
                Console.WriteLine("Absent");
            }
            else if (attendance == 1)
            {
                Console.WriteLine("Present");
            }

            Console.ReadLine();
        }
    }
}