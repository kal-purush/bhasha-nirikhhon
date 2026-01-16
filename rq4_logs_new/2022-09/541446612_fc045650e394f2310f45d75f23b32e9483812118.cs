namespace exercise5
{

    using System;
    using System.Collections.Generic;
    using System.Linq;
    public class Program
    {
        public static void Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine("Guess a number between 1-100?");
                int answer = Int32.Parse(Console.ReadLine());
                Random random = new Random(100);
                int number = random.Next(1, 100);
               /*  Console.WriteLine(number); */


                if (answer == number)
                {
                    Console.WriteLine("You won!");
                    break;
                }
                if (answer > number)
                {
                    Console.WriteLine($"{answer} is too high");
                }
                if (answer < number)
                {
                    Console.WriteLine($"{answer} is too low");
                }
            }

        }
    }
}