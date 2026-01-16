using System;
using System.Collections.Generic;
using System.Text;
using Lab04_Tic_Tac_Toe.Classes;

namespace Lab04_Tic_Tac_Toe.Classes
{
    public class Game
    {
        /// <summary>
        /// This takes in a board at any state in the game and prints it
        /// </summary>
        /// <param name="board">An array of arrays representing the state of the game</param>
        /// <returns>True for unit testing to see if this method works</returns>
        public static bool PrintGame (string[][] board)
        {
            foreach(string[] row in board)
            {
                foreach(string element in row)
                {
                    Console.Write($"| {element} | ");
                }
                Console.WriteLine();
            }
            return true;
        }

        /// <summary>
        /// This method holds the foundation for the game
        /// </summary>
        /// <param name="player1">Player 1 who is "X"</param>
        /// <param name="player2">Player 2 who is "O"</param>
        public static void GameStart(Player player1, Player player2)
        {
            string[][] board = GameBoard.Board();
            Console.WriteLine($"{player1.Name} is {player1.Icon}, {player2.Name} is {player2.Icon}");
            bool win = false;
            int counter = 0;
            while (!win)
            {
                Console.Clear();
                PrintGame(board);
                // Since both player1 and player2 start out with Turn = false,
                // This makes it so that it is player1's turn and I can keep it in the while loop.
                SwitchPlayer(player1, player2);

                if (player1.Turn)
                {
                    int userTile = TileChosen(player1, board);
                    board = GameMechanics(player1, board, userTile);
                }
                else
                {
                    int userTile = TileChosen(player2, board);
                    board = GameMechanics(player2, board, userTile);
                }
                Player Winner = CheckForWinner(player1, player2, board);
                if (!(Winner is null))
                {
                    Console.Clear();
                    PrintGame(board);
                    Console.WriteLine($"{Winner.Name} is the winner!");
                    win = true;
                    if (Winner.Name == "OctoCat")
                    {
                        Player.OctoCat();
                    }
                }
                // This counter is needed to make sure that the game ends when all 9 tiles have been played
                if (counter == 8)
                {
                    Console.Clear();
                    PrintGame(board);
                    Console.WriteLine("It's a draw!");
                    win = true;
                }
                counter++;
            }
        }

        /// <summary>
        /// This is how the game gets updated
        /// </summary>
        /// <param name="player">The player whose turn it is</param>
        /// <param name="board">The current state of the game</param>
        /// <param name="userTile">The tile that the user wants to mark</param>
        /// <returns>The updated state of the game</returns>
        public static string[][] GameMechanics (Player player, string[][] board, int userTile)
        {
            int[] position = GameBoard.Positions(userTile);
            // This identifies the exact location of where the player would place their icon.
            board[position[0]][position[1]] = player.Icon;
            return board;

        }

        /// <summary>
        /// This makes sure that the user picks a valid tile.
        /// </summary>
        /// <param name="player">The player whose turn it is</param>
        /// <param name="board">The current state of the board</param>
        /// <returns>A valid tile that the player picks</returns>
        static int TileChosen (Player player, string[][] board)
        {
            int userInput = -1;
            int[] position = new int[] { -1, -1 };

            // I used a double while loop because it was what I thought of at the time;
            // It is probably not the best idea but I would find time to refactor it if I remember in the future
            while (position[0] < 0)
            {
                // This while loop just makes sure that the user input is a value between 1 and 9.
                while (position[0] < 0)
                {
                    Int32.TryParse(Console.ReadLine(), out userInput);
                    position = GameBoard.Positions(userInput);
                    if (position[0] < 0)
                    {
                        Console.WriteLine("Invalid input, please pick a tile");
                    }
                }
                // This if statement checks to see if the tile that the player chose is occupied or not
                if (board[position[0]][position[1]] == "X" || board[position[0]][position[1]] == "O")
                {
                    Console.WriteLine("That tile is already taken, please pick another tile");
                    position[0] = -1;
                    position[1] = -1;
                }
            }

            return userInput;
        }

        /// <summary>
        /// This is the method for switching players
        /// </summary>
        /// <param name="player1">Player 1</param>
        /// <param name="player2">Player 2</param>
        public static void SwitchPlayer (Player player1, Player player2)
        {
            if (player1.Turn)
            {
                player1.Turn = false;
                player2.Turn = true;
                Console.WriteLine($"{player2.Name}, please pick a tile");
            }
            else
            {
                player1.Turn = true;
                player2.Turn = false;
                Console.WriteLine($"{player1.Name}, please pick a tile");
            }
        }

        /// <summary>
        /// This method checks for a winner after every round based off of the markers on the board
        /// </summary>
        /// <param name="player1">Player 1</param>
        /// <param name="player2">Player 2</param>
        /// <param name="board">The current state of the board</param>
        /// <returns>The player that has won (or null if there is no winner yet)</returns>
        public static Player CheckForWinner(Player player1, Player player2, string[][] board)
        {
            Player Winner;
            int[][] winners = GameBoard.Winners();
            // This for loop makes it so that every winning combination is tried
            for (int i = 0; i < winners.Length; i++)
            {
                // These 3 arrays hold the x/y coordinates for a winning sequence.
                int[] p1 = GameBoard.Positions(winners[i][0]);
                int[] p2 = GameBoard.Positions(winners[i][1]);
                int[] p3 = GameBoard.Positions(winners[i][2]);

                // These 3 strings find the matching icon for this winning sequence
                string a = board[p1[0]][p1[1]];
                string b = board[p2[0]][p2[1]];
                string c = board[p3[0]][p3[1]];

                // This if statement checks to see if all 3 of the icons are the same
                if ((a == b) && (b == c))
                {
                    // This tertiary statement returns the correct winner based off of whether the 3 correct icons are "X" or not.
                    Winner = (a == "X") ? player1 : player2;
                    return Winner;
                }
            }
            
            // If there is no winning combination, set the Winner player to be null.
            return Winner = null;
        }
    }
}