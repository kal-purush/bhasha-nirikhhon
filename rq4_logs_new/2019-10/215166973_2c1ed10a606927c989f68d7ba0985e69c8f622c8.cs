using System;

namespace TicTacToe
{
    class TicTacToe
    {
        static void Main(string[] args)
        {

            while (!isHorizontalWin())
            {
                buildBoard();
                placeMark();
            }

 
            /*
            do
            {
                buildBoard();
                getInput();
                 } while (!checkForWin() && !checkForTie());

                Console.ReadLine();
            }*/
        }

        public static string currentPlayer;
        public static int count = 1;
        public static string[][] board = new string[][]
        {
            new string[] {" ", " ", " "}, 
            new string[] {" ", " ", " "}, 
            new string[] {" ", " ", " "}
        };




        
        /*public static void getInput()
        {
            Console.WriteLine("Enter in a row.");
            int row = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter in a column.");
            int column = int.Parse(Console.ReadLine());
        }*/
        

        public static void buildBoard()
        {
            Console.WriteLine(" 1 2 3");
            Console.WriteLine(" " + string.Join('|', board[0]));
            Console.WriteLine(" -----");
            Console.WriteLine(" " + string.Join('|', board[1]));
            Console.WriteLine(" -----");
            Console.WriteLine(" " + string.Join('|', board[2]));

          
        }

        public static void placeMark()
        {
            Console.WriteLine("Enter in a row.");
            int row = int.Parse(Console.ReadLine());
            Console.WriteLine("Enter in a column.");
            int column = int.Parse(Console.ReadLine());

            if (board[row - 1][column - 1] == " ")
            {
                if (count % 2 == 0)
                {
                    currentPlayer = "O";
                    count += 1;
                }
                else
                {
                    currentPlayer = "X";
                    count += 1;
                }
                board[row - 1][column - 1] = currentPlayer;
            }
            else
            {
                placeMark();
            }


        }

        public static bool isHorizontalWin()
        {
            bool result = false;
            for(int i = 0; i<3; i++)
            {
                if (board[0][i] == "O")
                {
                    result = true;
                }
                else
                {
                    result = false;
                }
            }
            for (int i = 0; i < 3; i++)
            {
                if (board[1][i] == "O")
                {
                    result = true;
                }
                else
                {
                    result = false;
                }
            }
            for (int i = 0; i < 3; i++)
            {
                if (board[2][i] == "O")
                {
                    result = true;
                }
                else
                {
                    result = false;
                }
            }
            return result;
        }

    }
}