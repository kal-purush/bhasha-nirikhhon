using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using Microsoft.VisualBasic;
using GAME;

namespace FILEINTERACTION
{
    public class FileItem
    {
        public static int highScore = 0;
        public static List<int> totalHighscores = new List<int>();

        public static void OutputToFile(string score) {
            File.AppendAllText(@"highscore.txt", score.ToString() + Environment.NewLine);
        }

        public static int ReadFile()
        {
            StreamReader sr = new StreamReader("highscore.txt");
            String line = string.Empty;
            line = sr.ReadLine();
            while (line != null)
            {
                line = sr.ReadLine();

                if (int.TryParse(line, out int result))
                {
                    totalHighscores.Add(result);
                    if (result < highScore)
                        highScore = result;
                }
            }
            sr.Close();
            return highScore;
        }

        public static void OutputFileContent()
        {
            StreamWriter sw = new StreamWriter("highscore.txt");
            foreach (int score in totalHighscores)
            {
                sw.WriteLine(score);
            }
            sw.Close();
        }
    }

}