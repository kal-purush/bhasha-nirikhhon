using System;

namespace Exercise8
{
    class Program
    {
        const int maximumNumberOfTries = 10;
        private static int tryNumber = 0;
        private static string chosenWord;
        private static char[] guesses = new char[maximumNumberOfTries + 2];

        static void Main(string[] args)
        {
            string[] words = new string[] { "butterfly", "hedgehog", "hare", "alien", "rhino", "elephant", "fox", "chicken", "human", "monkey", "crane", "pigeon", "lizard", "worm", "fly", "wolf", "bear", "eagle", "crocodile", "cayman", "mosquito", "luntik", "fish", "lion", "dog", "cat", "kangaroo", "horse", "deer", "gorilla", "whale", "dolphin", "koala", "camel", "tiger", "rabbit", "zebra", "panda", "sheep", "leopard", "squirrel", "giraffe", "ant", "goat", "duck", "pig", "bee", "turkey", "cow", "rooster", "goat", "vulture", "snake", "ostrich", "bull", "hen", "hyena" };

            Random rnd = new Random();
            chosenWord = words[rnd.Next(words.Length)].ToUpper();

            string messageToUser = "";
            Console.WriteLine($"Try to guess an animal. You have maximum {maximumNumberOfTries} tries to guess!");
            DisplayWord();
            string letter;
            do
            {
                tryNumber++;
                do
                {
                    messageToUser = "";
                    Console.WriteLine($" {maximumNumberOfTries-tryNumber+1} tries left. Guess the letter: ");               
                    letter = Console.ReadLine().ToUpper();                   
                    if ((int)letter[0]<65 || (int)letter[0] > 91) messageToUser = "Please enter a valid letter!";
                    else if (Array.IndexOf(guesses, letter[0])>0) messageToUser = "This letter has already been called. Please enter another one!";
                    Console.WriteLine(messageToUser);
                } while (messageToUser != "");

                guesses[tryNumber] = letter[0];              
                DisplayWord();

            } while (!HaveWon() && tryNumber < maximumNumberOfTries);

            if (HaveWon()) messageToUser = "You have guessed the word!";
            else messageToUser = "You have lost! It was " + chosenWord;

            Console.WriteLine(messageToUser);
            Console.ReadKey();
        }

        private static void DisplayWord()
        {
            string word = "";
            char charAdd;
            for (int i = 0; i < chosenWord.Length; i++)
            {
                charAdd = '_';
                for (int j = 1; j <= tryNumber; j++)
                {
                    if (guesses[j] == chosenWord[i]) charAdd = chosenWord[i];
                }
                word += charAdd + " ";
            }
            Console.WriteLine(word);
        }
        private static bool HaveWon()
        {
            char charAdd;
            bool result = true;
            for (int i = 0; i < chosenWord.Length; i++)
            {
                
                charAdd = '_';
                for (int j = 1; j <= tryNumber; j++)
                {
                    if (guesses[j] == chosenWord[i]) charAdd = chosenWord[i];
                }
                if (charAdd == '_')
                {
                    result = false;
                    break;
                }
            }
            return result;
        }
    }
}