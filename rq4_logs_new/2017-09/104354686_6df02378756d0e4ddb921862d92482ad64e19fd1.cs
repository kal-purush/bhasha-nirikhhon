using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Game
{
    class Draw
    {
        public static void DrawScreen(char[,] boardArray, string controls)
        {
            var stopwatch = new Stopwatch();
            Console.Clear();
            stopwatch.Start();

            string[] line = new string[boardArray.GetLength(0)];
            string output = "";

            for (int i = 0; i < boardArray.GetLength(0); i++)
            {
                for (int j = 0; j < boardArray.GetLength(1); j++)
                {
                    line[i] += boardArray[i, j];
                }
            }
            stopwatch.Start();
            for (int i = 0; i < line.Length; i++) output += line[i] + "\n";

            Console.WriteLine(output + "\n" + controls);
            while (stopwatch.ElapsedMilliseconds <= 100) ;
            stopwatch.Stop();
        }
        public static void DrawGameOver(int highscore)
        {
            Console.WriteLine($"Game Over! You got the score: {highscore}");
        }
    }
}