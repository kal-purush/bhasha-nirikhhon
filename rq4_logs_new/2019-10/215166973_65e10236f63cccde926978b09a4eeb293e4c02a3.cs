using System;

namespace LCA_Csharp
{
    internal class RockPaperScissors
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Choose a number to make a selection 1)Rock, 2)Paper, 3)Scissors");
            string Playeroption = Console.ReadLine();
            Comparehands(Playeroption);

            void Comparehands(string Playeroption)
            {
                int Userpoint = 0;
                int Compoint = 0;
                int Rock = 1;
                int Paper = 2;
                int Scissors = 3;

                Random Comselection = new Random();
                int Computerhand = Comselection.Next(1, 4);

                switch (Computerhand)
                {
                    case 1:
                        Console.WriteLine("Computer chose rock");
                        if (Playeroption == "rock")
                        {
                            Console.WriteLine("Draw");
                        }
                        else if (Playeroption == "paper")
                        {
                            Console.WriteLine("You win!");
                            Userpoint++;
                        }
                        else if (Playeroption == "scissors")
                        {
                            Console.WriteLine("You lose");
                            Compoint++;
                        }
                        break;

                    case 2:
                        Console.WriteLine("Computer chose paper");
                        if (Playeroption == "rock")
                        {
                            Console.WriteLine("You lose");
                            Compoint++;
                        }
                        else if (Playeroption == "paper")
                        {
                            Console.WriteLine("Draw");
                        }
                        else if (Playeroption == "scissors")
                        {
                            Console.WriteLine("You win!");
                            Userpoint++;
                        }
                        break;

                    case 3:
                        Console.WriteLine("Computer chose scissors");
                        if (Playeroption == "rock")
                        {
                            Console.WriteLine("You win!");
                            Userpoint++;
                        }
                        else if (Playeroption == "paper")
                        {
                            Console.WriteLine("You lose");
                            Compoint++;
                        }
                        else if (Playeroption == "scissors")
                        {
                            Console.WriteLine("Draw");
                        }
                        break;

                        Console.WriteLine(switch);
        }
    }
}
    }
}