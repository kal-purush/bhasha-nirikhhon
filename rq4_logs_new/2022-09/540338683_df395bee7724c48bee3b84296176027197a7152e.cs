using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NumbersGame
{
    internal class Game
    {
        /*
         * Returns true if guess is right. Prints different responses if
         * guess is right, lower or higher.
         */
        public static bool CheckGuess(int guess, int answer)
        {
            var diff = guess - answer;
            string message = diff switch
            {
                < 0 => "Sorry, you guessed too low!",
                > 0 => "Sorry, you guessed too high!",
                _ => "Woho! You made it!"
            };
            Console.WriteLine(message);
            return guess == answer;
        }
    }
}