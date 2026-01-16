using System;
using CastleGrimtol.Services;

namespace CastleGrimtol
{
	class Program
	{
		static void Main(string[] args)
		{
			CastleService cs = new CastleService();
			Console.Clear();
			Console.WriteLine("Welcome to Castle Grimtol!");
			Console.WriteLine("Would you like to start a New Game? (Y/N)");
			while (cs.InStartMenu)
			{
				string input = Console.ReadLine().ToLower();
				if (input == "y" || input == "yes")
				{
					cs.InStartMenu = false;
					Console.Clear();
					Console.WriteLine("What is thy name brave one?");
					var name = Console.ReadLine();
					cs.StartNewGame(name);
					Console.Clear();
					Console.WriteLine($"{cs.GetPlayerName()}, Brave Young Warrior.");
					Console.WriteLine("Our forces are failing and the enemy grows stronger everyday.");
					Console.WriteLine("I fear if we don't act now our people will be driven from their homes.");
					Console.WriteLine("These dark times have left us with one final course of action.");
					Console.WriteLine("We must cut the head off of the snake by assasinating the Dark Lord of Grimtol...");
					Console.WriteLine("Our sources have identified a small tunnel that leads into the rear of the castle.");
					Console.WriteLine("Hurry! (type RUN)");
					string running = "";
					do
					{
						running = Console.ReadLine().ToLower();
						if (running == "run")
						{
							Console.Clear();
							Console.WriteLine("You've made it in without being spotted!");
							Console.WriteLine("You must now find a way to kill the Dark Lord without out being discovered. GOOD LUCK!");
							Console.ReadKey();
							Console.Clear();
							Console.WriteLine("Type HELP at anytime during the game to get a list of commands");
							Console.ReadKey();
						}
						else
						{
							Console.WriteLine($"Hurry {cs.GetPlayerName()} you dont have much time.");
						}
					} while (running != "run");
					cs.Playing = true;
					cs.StartGame();
				}
				else if (input == "n" || input == "no")
				{
					Console.Clear();
					Console.WriteLine("See you soon ol' sport!");
					cs.InStartMenu = false;
				}
				else if (input != "n" || input != "y")
				{
					Console.WriteLine("Invalid input. Please type yes or no.");
				}



			}
		}
	}
}