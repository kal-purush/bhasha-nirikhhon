using System;
using System.Collections.Generic;

namespace Mastermind
{
    class Program
    {
        /*
         Create a C# console application that is a simple version of Mastermind.  
         The randomly generated answer should be four (4) digits in length, with each digit between the numbers 1 and 6.
         After the player enters a combination, 
         a minus (-) sign should be printed for every digit that is correct but in the wrong position, and 
         a plus (+) sign should be printed for every digit that is both correct and in the correct position.  
         Nothing should be printed for incorrect digits.  
         The player has ten (10) attempts to guess the number correctly before receiving a message that they have lost. 
         */

        //Restarted 11:15PM 8/20 using code from oldprogram.txt - started to get carried away designing a lot more than requested and decided it would be better to keep it smaller and cleaner for now
        //Ended development and testing at 1:01AM 8/21
        //Finished comments at 1:21AM 8/21 


        static void Main()
        {
            string solution = createNewSolution();
            playGame(solution);
            Console.WriteLine("Press enter to exit the program.");
            Console.ReadLine();
            Environment.Exit(0);
        }

        static string createNewSolution()
        {
            /* The randomly generated answer should be four (4) digits in length, with each digit between the numbers 1 and 6. */
            string solutionString = "";
            Random soluRandom = new Random();
            List<string> allowedDigits = new List<string>() { "1", "2", "3", "4", "5", "6" };
            for (int i = 0; i < 4; i++)
            {
                //using Random.Next method allows random generation between min and max
                int numChosen = soluRandom.Next(0, allowedDigits.Count); //number chosen
                solutionString += allowedDigits[numChosen]; //adds the number to the solution
                allowedDigits.RemoveAt(numChosen); //Removes the digit from the list so no duplicates are used - list resizes
            }
            //now that the solution has been created, return it to the program for use.
            return solutionString;
        }
        static void playGame(string solution)
        {
            /* The player has ten (10) attempts to guess the number correctly before receiving a message that they have lost. */
            int totalGuesses = 10;  //used to make sure only the correct attempts are made.
            int userGuesses = 0;    //used to track how many guesses the player has been made.
            char[] guess;           //char array container for the users' guess
            bool results = false;   //results boolean to track if the player guesses correctly or runs out of guesses.
            introDescription();

            while (userGuesses < totalGuesses)    //loops the guess entry part while the user has guesses available
            {
                guess = new char[4]; //guess char array is cleared and replaced with a new one in every guess so nothing passes through between guesses
                userGuesses += 1;    //increment guess count
                Console.Write("Guess #" + userGuesses + ":\n");
                for (int i = 0; i < guess.Length; i++)
                {
                    //adds key pressed to collective guess array
                    guess[i] = Console.ReadKey().KeyChar;

                }
                string checkResult = checkGuess(guess, solution); //runs cgheck result method and gets the results (+, - or blank space for the numbers entered)
                Console.WriteLine("\n" + checkResult); //print results
                if (checkResult.Equals("++++"))
                {
                    //if the guess is all correct #'s in correct order, ++++ gets returned, and this shows that the player has won. while loop is left to end the game
                    results = true;
                    break;
                }
            }

            if (results)
            {
                Console.WriteLine("You correctly guessed the solution. Congratulations, you won!");
            }
            else
            {
                Console.WriteLine("You were not able to correctly guess the solution. You lost.");
                //Console.Write("\nThe Solution this game was: "+solution); //this can be uncommented at the start to show the solution to the user.
            }
        }

        static void introDescription()
        {
            //simple method to display a preface of instructions to the player.
            Console.WriteLine("Welcome to Mastermind." +
                "\nA random 4-number solution has been created using a combination of the digits 1, 2, 3, 4, 5 and 6. There are no duplicate digits." +
                "\nYou have 10 tries to guess the correct solution before the game is over.");
        }

        static string checkGuess(char[] guess, string solution)
        {
            int testGuessInSolutionPlace;                   //variable to track the idnex of where the user's entered guess # is in the solution, it at all
            char[] guessResults = new char[4];              //char array to contain the individual results of each guessed number
            char[] solutionChars = solution.ToCharArray();  //convert solution numbers to char array for easier searching and matching
            for (int i = 0; i < guess.Length; i++)
            {
                testGuessInSolutionPlace = 0;                 //default 0 is set so nothing accidentally passes around between checks

                //gets the index of the character we are checking for in the solution. It will return index # (0-3), or -1 if it is not found
                testGuessInSolutionPlace = Array.IndexOf(solutionChars, guess[i]);

                if (testGuessInSolutionPlace >= 0)
                {
                    if (i != testGuessInSolutionPlace) //checks if the index of the guessed # is in the same spot as in the solution
                    {
                        /* After the player enters a combination, a minus(-) sign should be printed for every digit that is correct but in the wrong position */
                        guessResults[i] = '-';
                    }
                    else
                    {
                        /* and a plus (+) sign should be printed for every digit that is both correct and in the correct position */
                        guessResults[i] = '+';
                    }

                    //Even though the code uses no duplicates, we clear any found characters so that there are no false positives
                    //i.e. if the code is 1234, and the user types in 1111, this code ensures that the result is "+    " and not "+---"
                    solutionChars[testGuessInSolutionPlace] = '\0';
                }
                else
                {
                    /* Nothing should be printed for incorrect digits.   */
                    guessResults[i] = ' ';
                }
            }
            return new string(guessResults); //return results for player
        }
    }
}