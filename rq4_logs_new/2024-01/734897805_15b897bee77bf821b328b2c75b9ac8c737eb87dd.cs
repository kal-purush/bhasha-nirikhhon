using System;
using System.Collections.Generic;

class Program
{
    static List<string> contestants = new List<string>();
    
    static void Main()
    {
        while (true)
        {
          Console.Write("Enter a name (leave blank to exit program): "); 
          string? name = Console.ReadLine();

          if (string.IsNullOrWhiteSpace(name))
              break;
          
          contestants.Add(name);
        }

        if (contestants.Count == 0)
        {
            Console.WriteLine("There are no contestants, exiting the program... ");
        }
        else
        {
            SelectAndDisplayWinner();
        }
    }
    static void SelectAndDisplayWinner()
    {
        Random random = new Random();
        int winnerIndex = random.Next(contestants.Count);
        string winner = contestants[winnerIndex];
        Console.WriteLine($"The winner is.... {winner}");
    }
}