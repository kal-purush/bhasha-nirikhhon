using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace TicTacToe
{
    static class Logic
    {


        public static string[,] GameField { get; set; }

        public static void CheckCell(int x, int y)
        {
            
            if (string.IsNullOrWhiteSpace(GameField[x, y]))
            {
                GameField[x, y] = DrawIcon();
            }
            else
            {
                MessageBox.Show($"Field taken");
            }

        }

        public static void setFieldSize(int size)
        {
            GameField = new string[size, size];
        }

        public static string DrawIcon()
        {
            return "X";
        }
    }
}