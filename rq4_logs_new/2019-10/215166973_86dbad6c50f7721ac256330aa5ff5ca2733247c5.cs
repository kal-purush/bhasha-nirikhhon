using System;
using System.Collections.Generic;

namespace masterMind
{
    class Program
    {
        static List<string> colors = new List<string> { "Red", "Yellow", "Blue" };
        static Random compSelect = new Random();

        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to Mastermind!\nThe computer will randomly choose 2 out of 3 colors.\nRed, Yellow, or Blue");
            int colorIndex = compSelect.Next(colors.Count);
            var color1 = colors[colorIndex];
            colorIndex = compSelect.Next(colors.Count);
            var color2 = colors[colorIndex];
            Console.WriteLine("Enter in your guess");
            Console.ReadLine();
            //Console.WriteLine($"The colors randomly chosen are {color1} and {color2}");





        }







    }
}