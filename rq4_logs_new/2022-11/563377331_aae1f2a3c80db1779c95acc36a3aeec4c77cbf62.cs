using System;
using System.Collections.Generic;
using System.Text;
namespace ProjectBank
{
    public class ViewUI
    {
        private string title;
        private string body;
        private string[] Options;
        private int SelectedOption = 0;
        public ViewUI(string title, string[] menuOptions)
        {
            this.title = title;
            this.Options = menuOptions;
        }
        public ViewUI(string title, string[] menuOptions, string body)
        {
            this.title = title;
            this.Options = menuOptions;
            this.body = body;
        }
        void UpdateUi()
        {
            Console.WriteLine(title + "\n");
            Console.WriteLine(body + "\n");
            //---Render Menu---
            for (int i = 0; i < Options.Length; i++)
            {
                if (i == SelectedOption)
                {
                    Console.ForegroundColor = ConsoleColor.Black;
                    Console.BackgroundColor = ConsoleColor.White;
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.BackgroundColor = ConsoleColor.Black;
                }
                Console.WriteLine($"[{Options[i]}]");//Menu button                                          
            }
            Console.ResetColor();
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine("\nNavigate with arrow keys and select with Enter");
            Console.ResetColor();
        }
        public int Run()
        {
            ConsoleKey keyPressed;
            do
            {
                Console.Clear();
                UpdateUi();
                ConsoleKeyInfo keyInfo = Console.ReadKey();
                keyPressed = keyInfo.Key;
                //---Menu input---
                if (keyPressed == ConsoleKey.DownArrow)
                {
                    SelectedOption++;
                }
                else if (keyPressed == ConsoleKey.UpArrow)
                {
                    SelectedOption--;
                }
                if (SelectedOption == Options.Length)
                {
                    SelectedOption = 0;
                }
                else if (SelectedOption < 0)
                {
                    SelectedOption = Options.Length - 1;
                }
            } while (keyPressed != ConsoleKey.Enter);
            return SelectedOption;
        }
    }
}