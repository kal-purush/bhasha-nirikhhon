using System;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Let's play a guessing game about me!");
            Console.WriteLine("First, what is your name?");
            string userName = Console.ReadLine();
            Console.WriteLine($"Thank you {userName}, let's begin.");
            Console.Read();
            CityBorn();
            NumberOfToppings();
            KillForPizza();
            Total();
            Console.WriteLine($"Game Over {userName}, thanks for playing!");
            Console.Read();
        }

        static int CityBorn()
        {
            Console.WriteLine("Was I born in Washington?");
            Console.Read();
            int counter = 0;
            string city = Console.ReadLine().ToLower();
            //Console.Read();

            if (city == "yes" || city == "y")
            {
                Console.WriteLine("Nope!  I was born in Colorado! In Denver.");

                Console.Read();

            }
            else if (city == "no" || city == "n")
            {
                Console.WriteLine("Correct! I was born in Denver, Colorado.");
                Console.Read();
                counter++;
            }
            else
            {
                Console.WriteLine("Please answer with 'yes' or 'no'.");
                CityBorn();
            }
            return counter;
        }

        static int KillForPizza()
        {
            Console.WriteLine("Would I kill for some pizza?");
            Console.Read();
            int counter;
            string kill = Console.ReadLine().ToLower();
            //Console.Read();

            if (kill == "yes" || kill == "y")
            {
                Console.WriteLine("Um, no, murder is illigal! But I really do want some pizza right now.");
                Console.Read();

            }
            else if (kill == "no" || kill == "n")
            {
                Console.WriteLine("Right, because murder is illigal, but I really do want some pizza right now.");
                Console.Read();
                counter++;
            }
            else
            {
                Console.WriteLine("Please answer with 'yes' or 'no'.");
                KillForPizza();
            }
            return counter;
        }

        static int NumberOfToppings()
        {

            int guesses = 0;
            int counter;
            do
            {
                Console.WriteLine("How many toppings do I prefer on my pizza?");
                Console.ReadLine();
                var userInput = Console.ReadLine();
                int numToppings;
                bool result = Int32.TryParse(userInput, out numToppings);
                if (result == false)
                {
                    Console.WriteLine("Please enter a number");
                    Console.Read();
                }


                if (numToppings < 4)
                {
                    Console.WriteLine("That's almost plain! More please!");
                    Console.Read();
                    guesses++;

                }
                if (numToppings > 4)
                {
                    Console.WriteLine("Hey, don't overload the pie! Try again.");
                    Console.Read();
                    guesses++;

                }
                if (numToppings == 4)
                {
                    Console.WriteLine("Bingo, I think 4 toppings in optimal pizza yummage!");
                    Console.Read();
                    counter++;
                    break;
                }

            }
            while (guesses < 5);
            if (guesses == 5)
                Console.WriteLine("Sorry, you are out of guesses.  I was looking for 4 topppings.");
            Console.Read();
            return counter;
        }

        static void Total()
        {
            int total = CityBorn() + NumberOfToppings() + KillForPizza();
            Console.WriteLine($"You got {total} questions right!");
        }
    }
}