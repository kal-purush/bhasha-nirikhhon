using System;
using System.IO;

namespace ConsoleApp2
{
    class Program
    {
        static void Main(string[] args)
        {
            int collumns = 5;
            int cells = 30;
            string[] repeatingStrings = { "Arms & Back", "Legs", "Abs & Core"};

            #region Generator
            string result = @"
                <!DOCTYPE html>
                <html>
                <style>
                    table {
                        width: 100%;
                        border-collapse: collapse;
                        cellspacing: 0;
                    }
                    td {
                        border: 1px solid black;
                        padding: 5px;
                        text-align: center;
                        vertical-align: top;
                    }
                </style>
                <body>
                <table>
                ";
            int rows = (int)Math.Ceiling((double)cells / (double)collumns);
            for (int i = 0; i < rows; i++)
            {
                result += "<tr>\n";
                for (int j = 0; j < collumns; j++)
                {
                    string cell_content = "";
                    int cell_num = i * collumns + (j + 1);
                    if (cell_num <= cells)
                    {
                        int number = cell_num - (int)Math.Floor((double)(cell_num - 1) / repeatingStrings.Length) * repeatingStrings.Length - 1;
                        cell_content = $"<h1>{cell_num}</h1>"
                        + $"<h3>{repeatingStrings[number]}</h3>";
                    }

                    result += $"<td>{cell_content}</td>";
                }
                result += "</tr>\n";
            }

            result += "</table>\n</body>\n</html>";
            File.WriteAllText(Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + "\\result.html", result);
            #endregion
        }
    }
}