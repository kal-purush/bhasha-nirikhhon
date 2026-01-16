using System;

namespace Excercise5
{
    class Program
    {
        static void Main(string[] args)
        {
            const int numberOfGuesses = 2;
            Random rnd = new Random();
            
            for (int i=1; i<=numberOfGuesses;i++)
            {
                int randomNumber = rnd.Next(1, 101);
                Console.WriteLine("I'm thinking of a number between 1-100.  Try to guess it.");
                int number = Int32.Parse(Console.ReadLine());
                if (number < randomNumber) Console.WriteLine($"Sorry, you are too low.  I was thinking of {randomNumber}");
                if (number > randomNumber) Console.WriteLine($"Sorry, you are too high.  I was thinking of {randomNumber}");
                if (number == randomNumber) Console.WriteLine($"You guessed it!  What are the odds?!?");
            }
            Console.ReadKey();

        }                     
        
    }
}