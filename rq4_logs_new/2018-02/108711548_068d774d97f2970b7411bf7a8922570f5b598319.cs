using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hangman
{
    class Program
    {
        static void Main(string[] args)
        {
            Random rnd = new Random();
            List<string> words = new List<string>();

            words.Add("computer");
            words.Add("ceiling");
            words.Add("canvas");
            words.Add("victory");
            words.Add("machine");
            words.Add("apartment");
            words.Add("wardrobe");
            words.Add("hurricane");
            words.Add("alligator");
            words.Add("democracy");
            words.Add("voltage");
            words.Add("alcohol");
            words.Add("intelligence");
            words.Add("continent");
            words.Add("pentagram");

            string solution = words[rnd.Next(words.Count())];
            char startletter = solution[rnd.Next(solution.Length)];
            char[] parts = new char[solution.Length];
            for (int i = 0; i < parts.Length; i++) parts[i] = '_';
            RevealLetters(solution, parts, startletter);
            int lives = 5;
            var guessed = new List<char>();
            guessed.Add(startletter);

            Console.WriteLine("Welcome to Hangman! Reveal the word by guessing letters.");
            Console.WriteLine($"You can guess incorrectly {lives} times.");
            Console.WriteLine("The word is: ");
            Console.WriteLine();
            Console.WriteLine($"{string.Join(" ", parts)}");
            Console.WriteLine();

            while (true)
            {
                char guess = new char();
                Console.Write("Enter a letter: ");
                string input = Console.ReadLine();
                try
                {
                    guess = char.Parse(input);
                }
                catch

                {
                    Console.WriteLine("Incorrect input. Enter one letter.");
                    continue;
                }

                if (guessed.Contains(guess))
                {
                    Console.WriteLine("You've already tried that letter!");
                    continue;
                }

                guessed.Add(guess);

                if (solution.Contains(guess))
                {
                    Console.WriteLine("Good guess!");
                    RevealLetters(solution, parts, guess);

                    if (!parts.Contains('_'))
                    {
                        Console.WriteLine();
                        Console.WriteLine($"{string.Join(" ", parts)}");
                        Console.WriteLine();
                        Console.WriteLine("You've guessed the word!");

                        Console.WriteLine($"The word was: {solution}");
                        break;
                    }
                }

                else
                {
                    Console.WriteLine("Bad guess!");
                    lives--;
                    
                    if(lives < 0)
                    {
                        Console.WriteLine("You didn't guess the word!");

                        Console.WriteLine($"The word was: {solution}");
                        break;
                    }

                    Console.WriteLine($"Incorrect guesses remaining: {lives}");
                }

                Console.WriteLine();
                Console.WriteLine($"Letters tried: {string.Join(", ", guessed)}");
                Console.WriteLine();
                Console.WriteLine($"{string.Join(" ", parts)}");
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine("Press any key to continue");
            Console.ReadKey();
        }

        static void RevealLetters(string sol, char[] chars, char x)
        {
            for (int i = 0; i < sol.Length; i++)
            {
                if (sol[i] == x) chars[i] = x;
            }
        }
    }
}