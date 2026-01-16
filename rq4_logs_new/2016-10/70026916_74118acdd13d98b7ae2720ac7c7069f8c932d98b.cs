using System;


namespace TicTacToe
{
    class Program
    {
        static string[] board = { " ", " ", " ",
                                  " ", " ", " ",
                                  " ", " ", " " };
        static void Main(string[] args)
        {
            PrintBoard();
            Console.WriteLine("Shall we play a game?");
            Play();
        }

        static void Play()
        {
            Console.WriteLine("Enter a spot. \"x,y\"");
            char[] delim = { ',' };
            string[] positions = Console.ReadLine().Split(delim); // --> "1, 1" -> ["1", "2"]
            int x = Int32.Parse(positions[0]);
            int y = Int32.Parse(positions[1]);

            //validate user entry.
            if (!(validateInput(x) && validateInput(y)))
            {
                Play();
            }

            int index = GetIndex(x, y);
            if (isSpotOpen(index))
            {
                board[index] = "X";
            }
            else
            {
                Play();
            }

            if (checkForWinner())
            {
                PrintBoard();
                Console.WriteLine("Congratulations!! You are the winner.");
                resetBoard();
            }
            else
            {
                checkCatScratch();
            }
            opponentMove();
            PrintBoard(); // puts X at chosen spot
            Play();
        }

        static void opponentMove()
        {
            int[] openSpots = { -1, -1, -1, -1, -1, -1, -1, -1, -1 };
            int count = 0;
            int winningPosition = 0;
            for (int i = 0; i <= 8; i++)
            {
                if (board[i] == " ")
                {
                    openSpots[count] = i;
                    count = count + 1;
                }
            }
            //check for winning move for computer
            winningPosition = checkWinningMove("O", openSpots);
            if (winningPosition != -1)
            {
                board[winningPosition] = "O";
            }
            else
            {
                //Check for winning position for player
                winningPosition = checkWinningMove("X", openSpots);
                if (winningPosition != -1)
                {
                    board[winningPosition] = "O";
                }
                else
                {
                    // No winning position found; choose a random spot
                    Random rnd = new Random();
                    int randomInteger = rnd.Next(0, count);
                    board[openSpots[randomInteger]] = "O";
                }
            }
            if (checkForWinner() == true)
            {
                PrintBoard();
                Console.WriteLine("You lose!");
                resetBoard();
                Play();
            }
        }
        static int checkWinningMove(string playerMove, int[] openPositions)
        {
            int maxAvailable = 0;
            // Need to figure out how high the open position array goes.
            for (int i = 0; i <= openPositions.Length - 1; i++)
            {
                if (openPositions[i] != -1) { maxAvailable = i; };
            }

            for (int i = 0; i <= maxAvailable; i++)
            {
                if (board[openPositions[i]] == " ")
                {
                    board[openPositions[i]] = playerMove;
                    if (checkForWinner() == true)
                    {
                        return (openPositions[i]);
                    }
                }
                board[openPositions[i]] = " ";
            }
            return -1;
        }

        static int GetIndex(int x, int y)
        {
            return (x - 1) + (y - 1) * 3;
        }

        static void PrintBoard()
        {
            Console.WriteLine("-------------");
            Console.WriteLine("| {0} | {1} | {2} |", board[0], board[1], board[2]);
            Console.WriteLine("| {0} | {1} | {2} |", board[3], board[4], board[5]);
            Console.WriteLine("| {0} | {1} | {2} |", board[6], board[7], board[8]);
            Console.WriteLine("-------------");
        }

        static bool validateInput(int z)
        {
            if (z > 0 && z <= 3)
            {
                return true;
            }
            else
            {
                Console.WriteLine("Oops! Invalid move. Try again...");
                return false;
            }
        }

        static bool isSpotOpen(int i)
        {
            if (board[i] == "X" || board[i] == "O")
            {
                Console.WriteLine("Oops that spot is taken. Try again");
                return false;
            }
            else
            {
                return true;
            }

        }
        static bool checkForWinner()
        {
            if (
                (board[0] == board[1] && board[1] == board[2] && board[2] != " ") ||
                (board[3] == board[4] && board[4] == board[5] && board[5] != " ") ||
                (board[6] == board[7] && board[7] == board[8] && board[8] != " ") ||
                (board[0] == board[4] && board[4] == board[8] && board[8] != " ") ||
                (board[2] == board[4] && board[4] == board[6] && board[6] != " ") ||
                (board[0] == board[3] && board[3] == board[6] && board[6] != " ") ||
                (board[1] == board[4] && board[4] == board[7] && board[7] != " ") ||
                (board[2] == board[5] && board[5] == board[8] && board[8] != " ")
                )
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        static bool checkCatScratch()
        {
            if ((board[0] != " " && checkForWinner() == false) &&
                (board[1] != " " && checkForWinner() == false) &&
                (board[2] != " " && checkForWinner() == false) &&
                (board[3] != " " && checkForWinner() == false) &&
                (board[4] != " " && checkForWinner() == false) &&
                (board[5] != " " && checkForWinner() == false) &&
                (board[6] != " " && checkForWinner() == false) &&
                (board[7] != " " && checkForWinner() == false) &&
                (board[8] != " " && checkForWinner() == false))
            {
                Console.WriteLine("Catscratch.");
                resetBoard();
                return true;
            }
            else
            {
                return false;
            }
        }

        static void resetBoard()
        {
            Console.WriteLine("Would you like to play again? (y/n)");
            string answer = Console.ReadLine().ToUpper();
            if (answer == "Y")
            {
                board[0] = " ";
                board[1] = " ";
                board[2] = " ";
                board[3] = " ";
                board[4] = " ";
                board[5] = " ";
                board[6] = " ";
                board[7] = " ";
                board[8] = " ";
                PrintBoard();
                Play();
            }
            else
            {
                System.Environment.Exit(0);
            }
        }
    }
}