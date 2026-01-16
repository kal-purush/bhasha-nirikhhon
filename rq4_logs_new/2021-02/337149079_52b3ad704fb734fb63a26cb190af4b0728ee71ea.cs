using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace SpaceGame
{
    class Menu
    {

        public string Name { get; set; } = null;

        public bool GameOverFlag { get; set; } = false; // set to false so the game will run

        public difficulty difficultyLevel = difficulty.Easy;  // default difficulty of easy+

        private List<string> MainMenuOptions = new List<string>() { "New Game", "Load Game", "Quit" };

        private List<string> ResourceOptons = new List<string>()
            {"Dark Mater", "Gold", "Hull Integrity", "Resource Capacity"};

        public enum difficulty
        {
            Easy,
            Medium,
            Hard,
            Advanced
        }

        public void ShowMainMenu()
        {
            ShowBanner("Main Menu", 30);
            ShowOptions(MainMenuOptions);
            int option = GetUserInput(MainMenuOptions);
            Console.WriteLine(option);

        }

        private void ShowOptions(List<string> options, int? index = null)
        // Ex; (MenuOptions, null) => option1 \n option2 OR (MenuOptions, 1) => [1] Option1 \n [2] Option2
        {
            if (index != null)
            {
                int x = 1;
                foreach (string option in options)
                {
                    Console.WriteLine($"[{x.ToString()}] {option}");
                    x++;
                }

            }
            else
            {
                foreach (string option in options)
                {
                    Console.WriteLine($"{option}");
                }
            }
        }
        private void ShowBanner(string message, int length)
        {
            int frameSize = length;
            int center = ((frameSize / 2) + (message.Length / 2));
            message = message.PadLeft(center, '-').PadRight(frameSize, '-');
            Console.WriteLine(message);
        }

        private void ShowSeparator(char x, int length)
        {
            Console.WriteLine("".PadRight(length, x));
        }

        private int GetUserInput(List<string> menu)
        {
            string input = Console.ReadLine();
            int selection;
            while (true)
            {
                if (Int32.TryParse(input, out int j) == false)
                {
                    Console.WriteLine("Please enter a valid number");
                    Console.Write("\r" + new string(' ', Console.WindowWidth) + "\r");
                    input = Console.ReadLine();
                    Console.SetCursorPosition(0, Console.CursorTop - 2);
                    Console.Write("\r" + new string(' ', Console.WindowWidth) + "\r");
                }
                else if (j >= (menu.Count + 1) || j <= 0)
                {
                    Console.WriteLine("Number is not in menu range");
                    Console.Write("\r" + new string(' ', Console.WindowWidth) + "\r");
                    input = Console.ReadLine();
                    Console.SetCursorPosition(0, Console.CursorTop - 2);
                    Console.Write("\r" + new string(' ', Console.WindowWidth) + "\r");
                }
                else
                {
                    selection = j;
                    break;
                }
            }
            return selection;
        }

        //public void ShowPlanetMenu(Planet planet)
        //{
        //    foreach(KeyValuePair in planet)
        //}

        private void RunMainMenuOption(int option)
        // Main Menu only has 3 options, 1. New Game 2. Load Game 3. Quit
        {
            switch (option)
            {
                case 1:
                    //New Game
                    Console.WriteLine("Start new game here");
                    //TODO link with maksim class
                    break;
                case 2:
                    //Load Game
                    Console.WriteLine("Load game here");
                    //TODO serialize and load games
                    break;
                case 3:
                    Console.WriteLine("Thank you for playing!");
                    Environment.Exit(0);
                    break;
            }
        }



        public void GameOver()
        {
            //TODO
        }

        public void Credits()
        {
            //TODO
        }
    }
}