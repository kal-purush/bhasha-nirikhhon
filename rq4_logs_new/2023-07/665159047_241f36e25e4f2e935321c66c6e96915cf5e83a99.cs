using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankomatConsole.Menu
{
    static class AdvancedMenu
    {
        static string[] menuOptions = { "Withdraw cash (1)", "Deposit cash (2)", "Check balance (3)", "Exit (4)" };
        static int activeOption = 0;

        public static void StartMenu()
        {
            Console.Title = "YourATM by Michał Krężlewicz";
            Console.CursorVisible = false;
            while (true)
            {
                ShowMenu();
                ChooseOption();
                RunOption();
            }
        }

        static void ShowMenu()
        {
            Console.Clear();
            AsciiArt.DisplayAscii();
            Console.Write("".PadLeft(8));
            Console.WriteLine("Use arrow keys or number keys.");
            Console.WriteLine();
            for (int i = 0; i < menuOptions.Length; i++)
            {
                if (i == activeOption)
                {
                    Console.Write("".PadLeft(8));
                    Console.BackgroundColor = ConsoleColor.Blue;
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.WriteLine(menuOptions[i]);
                    Console.BackgroundColor = ConsoleColor.DarkBlue;
                    Console.ForegroundColor = ConsoleColor.White;
                }
                else
                {
                    Console.Write("".PadLeft(8));
                    Console.WriteLine(menuOptions[i]);
                }
            }
        }

        static void ChooseOption()
        {
            do
            {
                ConsoleKeyInfo key = Console.ReadKey();
                if (key.Key == ConsoleKey.UpArrow)
                {
                    activeOption = (activeOption > 0) ? activeOption - 1 : menuOptions.Length - 1;
                    ShowMenu();
                }
                else if (key.Key == ConsoleKey.DownArrow)
                {
                    activeOption = (activeOption + 1) % menuOptions.Length;
                    ShowMenu();
                }
                else if (key.Key == ConsoleKey.Escape)
                {
                    activeOption = menuOptions.Length - 1;
                    ShowMenu();
                }
                else if (key.Key == ConsoleKey.Enter)
                {
                    break;
                }
                else if (key.KeyChar >= '1' && key.KeyChar <= '4')
                {
                    int optionIndex = key.KeyChar - '1';
                    if (optionIndex < menuOptions.Length)
                    {
                        activeOption = optionIndex;
                        ShowMenu();
                        break;
                    }
                }
            }
            while (true);
        }


        static void RunOption()
        {
            switch (activeOption)
            {
                case 0: Console.Clear(); underConstructionWC(); break;
                case 1: Console.Clear(); underConstructionDC(); break;
                case 2: Console.Clear(); underConstructionCB(); break;
                case 3: Environment.Exit(0); break;
            }
        }

        static void underConstructionWC()
        {
            Console.SetCursorPosition(4, 1);
            Console.WriteLine("Withdraw cash option is still under construction");
            Console.SetCursorPosition(13, 3);
            Console.WriteLine(">>> PRESS ANY KEY TO EXIT <<<");
            Console.ReadKey();
        }

        static void underConstructionDC()
        {
            Console.SetCursorPosition(4, 1);
            Console.WriteLine("Deposit cash option is still under construction");
            Console.SetCursorPosition(13, 3);
            Console.WriteLine(">>> PRESS ANY KEY TO EXIT <<<");
            Console.ReadKey();
        }

        static void underConstructionCB()
        {
            Console.SetCursorPosition(4, 1);
            Console.WriteLine("Check Balance option is still under construction");
            Console.SetCursorPosition(13, 3);
            Console.WriteLine(">>> PRESS ANY KEY TO EXIT <<<");
            Console.ReadKey();
        }
    }
}