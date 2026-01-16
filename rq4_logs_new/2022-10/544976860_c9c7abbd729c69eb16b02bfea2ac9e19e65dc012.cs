using System;
using System.Collections.Generic;
using System.Text;

namespace TicTacToeWithAI
{
    class Menu
    {
        public Menu()
        {
            string option = "";
            do
            {
                Console.Clear();
                Console.WriteLine("1 - Jogar");
                Console.WriteLine("0 - Sair");
                Console.WriteLine("\nOpção: ");

                option = Console.ReadLine();

                if (option == "1")
                    new TicTacToe();
            
            } while (option != "0");         
        }
    }
}