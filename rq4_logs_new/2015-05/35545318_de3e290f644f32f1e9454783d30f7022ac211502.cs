using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Solution
{
    class Program
    {
        static string moveRobot(string s) 
        {
            if (string.IsNullOrEmpty(s))
                return "";
            s = s.Trim();

            string processedCommand = "";
            string cleanedCommand = "";
            int removeCommand = 0; 
            int x = 0; //Longitude
            int y = 0; //Latitude

            //Identify X and others commands
            for (int i = s.Length-1; i >= 0; i--)
            {
                string command = s[i].ToString();
                if (command == "X")
                {
                    removeCommand++;
                    int numero;
                    bool isNumeric = int.TryParse(s[i - 1].ToString(), out numero);
                    //remove number too
                    if (isNumeric)
                        i = i - 1;
                }
                else
                {
                    if (removeCommand > 0)
                        removeCommand--;
                    else
                        cleanedCommand = command + cleanedCommand;
                }
            }
            //process commands
            for (int i = 0; i < cleanedCommand.Length; i++)
            {
                int numero = 0;
                string command = "";
                string first = cleanedCommand[i].ToString();
                bool isNumeric = int.TryParse(first, out numero);
                if (isNumeric)
                {
                    command = cleanedCommand[i+1].ToString();
                    i = i + 1;
                }
                else
                    command = cleanedCommand[i].ToString();

                switch(command.ToUpper())
                {
                    case "N":
                        x = x + Math.Max(1,numero);
                        break;
                    case "S":
                        x = x - Math.Max(1, numero);
                        break;
                    case "E":
                        y = y + Math.Max(1, numero);
                        break;
                    case "W":
                        y = y - Math.Max(1, numero);
                        break;
                }
            }

            if (x  > 0 || y > 0)
                processedCommand = "(" + y + ","+ x + ")";

            int xx =x;
            int yy = 0 - y;
            return processedCommand;
        } 

        static void Main(string[] args)
        {
            string fileName = @"C:\test_cases_9696diqo\dario001.txt"; //System.Environment.GetEnvironmentVariable("OUTPUT_PATH");
            TextWriter tw = new StreamWriter(@fileName, true);
            string res;
            string _s;
            _s = Console.ReadLine();

            res = moveRobot(_s);
            tw.WriteLine(res);

            tw.Flush();
            tw.Close();
        }
    }
}