using System;
using System.Collections.Generic;
using System.Text;

// https://www.geeksforgeeks.org/minimax-algorithm-in-game-theory-set-3-tic-tac-toe-ai-finding-optimal-move/
namespace tic_tac_toe
{
    class Minimax
    {
        public static int Runs = 0;

        public static Position GetBestMove(Board board)
        {
            int bestValue = -1000;
            Position bestMove = new Position(-1, -1);

            for (int row = 0; row < board.BOARD_DIMENSION; row++)
            {
                for (int column = 0; column < board.BOARD_DIMENSION; column++)
                {
                    if (board.IsSpaceEmpty(row, column))
                    {
                        board.UpdateBoard(row, column, (State)Players.PlayerOne);
                        int moveValue = Minimax.MinimaxCheck(board, 0, false, 0);
                        board.UpdateBoard(row, column, State.Empty);

                        if (moveValue > bestValue)
                        {
                            bestMove = new Position(row, column);
                            bestValue = moveValue;
                        }
                    }
                }
            }
            Console.WriteLine($"The best move is {bestMove.Row} and {bestMove.Column}!");
            return bestMove;
        }

        public static int MinimaxCheck(Board board, int depth, bool maxComputer, int turnsTaken)
        {
            Console.WriteLine(Minimax.Runs);
            Minimax.Runs++;

            int score = Minimax.GetWin(board);

            // if there is a win or loss
            if (score == 10 || score == -10)
            {
                Console.WriteLine("WIN OR LOSE");
                return score;
            }

            // if the game is over without a winnder (draw)
            if (turnsTaken == Game.TURNS)
            {
                return 0;
            }

            // runs if it's the computer's turn
            if (maxComputer)
            {
                int currentBest = -1000;

                for (int row = 0; row < board.BOARD_DIMENSION; row++)
                {
                    for (int column = 0; column < board.BOARD_DIMENSION; column++)
                    {
                        if (board.IsSpaceEmpty(row, column))
                        {
                            board.UpdateBoard(row, column, (State)Players.PlayerTwo);
                        }

                        currentBest = Math.Max(currentBest, Minimax.MinimaxCheck(board, depth + 1, !maxComputer, depth + 1));

                        board.UpdateBoard(row, column, State.Empty);
                    }
                }

                return currentBest;
            } 
            else // runs if it's the player's turn
            {
                int currentBest = 1000;

                for (int row = 0; row < board.BOARD_DIMENSION; row++)
                {
                    for (int column = 0; column < board.BOARD_DIMENSION; column++)
                    {
                        if (board.IsSpaceEmpty(row, column))
                        {
                            board.UpdateBoard(row, column, (State)Players.PlayerOne);
                        }

                        currentBest = Math.Min(currentBest, Minimax.MinimaxCheck(board, depth + 1, !maxComputer, depth + 1));

                        board.UpdateBoard(row, column, State.Empty);
                    }
                }

                return currentBest;
            }
        }

        public static int GetWin(Board board)
        {
            if (board.IsTheWinner(Players.PlayerTwo))
            {
                return 10;
            }
            else if (board.IsTheWinner(Players.PlayerOne))
            {
                return -10;
            }
            else
            {
                return 0;
            }
        }
    }
}