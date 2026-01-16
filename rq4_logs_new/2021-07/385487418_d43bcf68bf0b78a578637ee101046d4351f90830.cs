using System;

namespace RollTwoDice
{
    class Program
    {
        static void Main(string[] args)
        {
            int sum = 2;
            do
            {
                Console.WriteLine("What do You desire to throw?");
                sum = Convert.ToInt32(Console.ReadLine());
            } while (sum <2 && sum >12);
            Random random = new Random();
            int dice1,dice2;
            do
            {
                dice1 = random.Next(1, 6);
                dice2 = random.Next(1, 6);
                Console.WriteLine($"{dice1} and {dice2} rolled result in {dice1+dice2}");
            } while (dice1 + dice2 != sum);                       
            Console.ReadKey();
        }
    }
}