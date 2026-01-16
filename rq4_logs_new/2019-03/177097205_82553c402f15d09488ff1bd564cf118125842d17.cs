using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TicTacToe
{
    static class DataHandler

    {

        static string filePath = "saveFile.txt";
        public static void SaveGame()
        {
            File.Delete(filePath);
            File.AppendAllText(filePath, Logic.GameToString());
            File.AppendAllText(filePath, Logic.FieldToString());

            //File.AppendText
        }

        public static void LoadGame()
        {
            string[] lines = File.ReadAllLines(filePath);
            string[] gameSettings = lines[0].Split('\t');
            //sb.AppendLine($"{NumberOfPlayers}\t{Turn}\t{WinLength}\t{FieldSize}");
            Logic.NumberOfPlayers = int.Parse(gameSettings[0]);
            Logic.Turn = int.Parse(gameSettings[1]);
            Logic.WinLength = int.Parse(gameSettings[2]);
            Logic.FieldSize = int.Parse(gameSettings[3]);
            for (int i = 1; i < Logic.FieldSize; i++)
            {
                string line = lines[i];
                string[] parts = line.Split('\t');
                for (int j = 0; j < Logic.FieldSize; j++)
                {
                    Logic.GameField[i, j] = parts[j];
                }
            }
        }
    }
}