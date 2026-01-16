using System;
using System.IO;
namespace WordGuessGame
{
    class Program
    {
        static string path = "../../../WordList.txt";

        static void Main(string[] args)
        {
            CreateFile();
            UpdateNewFile();
            MainMenu();
        }
        static void MainMenu()
        {
            Console.WriteLine("Welcome to my word guess game, Please select an option with 1, 2, or 3");
            Console.WriteLine("1. Play New Game");
            Console.WriteLine("2. Admin View");
            Console.WriteLine("3. Quit game");
            string input = Console.ReadLine();
            while (input != "1" && input !=  "2" && input != "3")
                {
                    Console.WriteLine("Please Select with 1, 2 or 3");
                    input = Console.ReadLine();
                }
            InputHandleMain(input);
            }

        static void InputHandleMain(string input)
        {
            if(input == "1")
            {
                //StartGame();
            }
            if(input == "2")
            {
                adminView();
            }
            if(input == "3")
            {
                Environment.Exit(0);
            }
        }
        static void StartGame()
        {
            string word = RandomWordSelect();
            
            bool finishedGame = false;
            while (!finishedGame)
            {

            }
        }
        static void RandomWordSelect()
        {

        }
        static void adminView()
        {
            Console.WriteLine("Welcome to admin view. What would you like to do?");
            Console.WriteLine("1. View word bank");
            Console.WriteLine("2. Add Word");
            Console.WriteLine("3. Reset word bank");
            string input = Console.ReadLine();
            while (input != "1" && input != "2" && input != "3")
            {
                Console.WriteLine("Please Select with 1, 2 or 3");
                input = Console.ReadLine();
            }
            if(input == "1")
            {
                ReadFile();
                adminView();
            }
            if(input == "2")
            {
                AddWord();
                adminView();
            }
            if(input == "3")
            {
                DeleteFile();
                CreateFile();
                UpdateNewFile();
                Console.WriteLine("Your word bank has now been reset to the default bank");
                adminView();
            }
        }
        static void AddWord()
        {
            Console.WriteLine("What word you like to add to the word bank?");
            string newWord = Console.ReadLine().ToLower();
            UpdateFile(newWord);
            Console.WriteLine($"{newWord} has now been added to the word bank.");
            adminView();
        }









        static void CreateFile()
        {
            if (!File.Exists((path)))
            {
                using (StreamWriter sw = new StreamWriter(path))
                {
                   // sw.Write(path);
                }
            }
        }
        static void ReadFile()
        {
        try
            {
                int wordCount = 0;
                string[] wordList = File.ReadAllLines(path);
                foreach (string value in wordList)
                {
                    wordCount++;
                    Console.WriteLine(value);
                }
                Console.WriteLine($"The word bank currently holds {wordCount} words");
            }
            catch (Exception e)
            {
                Console.WriteLine("Something went terribly wrong");
                Console.WriteLine(e);
            }

        }
        static void UpdateFile(string word)
        {
            using (StreamWriter sw = File.AppendText(path))
            {
                sw.WriteLine(word);
            }
        }
        static void UpdateNewFile()
        {
            using (StreamWriter sw = File.AppendText(path))
            {
                sw.WriteLine("monkey");
                sw.WriteLine("banana");
                sw.WriteLine("ocean");
                sw.WriteLine("coding");
                sw.WriteLine("trampoline");
            }
        }
        static void DeleteFile()
        {
            File.Delete(path);
        }

    }

}