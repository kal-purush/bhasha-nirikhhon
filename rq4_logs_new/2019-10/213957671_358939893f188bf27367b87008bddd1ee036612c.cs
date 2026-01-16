using System;

namespace tic_tac_toe
{
    public enum State { Empty, O, X };

    class Board
    {
        private State[,] GameBoard;
        private int BOARD_DIMENSION = 3;

        internal Board()
        {
            GameBoard = new State[BOARD_DIMENSION, BOARD_DIMENSION];
        }

        internal bool IsSpaceEmpty(int userInput)
        {
            Position spaceToCheck = new Position(userInput);
            return GameBoard[spaceToCheck.Row, spaceToCheck.Column] == State.Empty ? true : false;
        }

        internal void UpdateBoard(int userInput, Players activePlayer)
        {
            Position spaceToUpdate = new Position(userInput);
            State playerSign = (State)activePlayer;
            GameBoard[spaceToUpdate.Row, spaceToUpdate.Column] = playerSign;
            PrintBoard(activePlayer);
        }

        internal void PrintBoard(Players activePlayer)
        {
            Console.Clear();

            Console.WriteLine($" {StateToString(0, 0)} | {StateToString(0, 1)} | {StateToString(0, 2)}");
            Console.WriteLine("---+---+---");

            Console.WriteLine($" {StateToString(1, 0)} | {StateToString(1, 1)} | {StateToString(1, 2)}");
            Console.WriteLine("---+---+---");

            Console.WriteLine($" {StateToString(2, 0)} | {StateToString(2, 1)} | {StateToString(2, 2)}");
        }

        public string StateToString(int row, int col)
        {
            switch (GameBoard[row, col])
            {
                case State.Empty:
                    return " ";
                case State.O:
                    return "O";
                default:
                    return "X";
            }
        }

        internal bool IsTheWinner(Players activePlayer)
        {
            State playerSign = (State)activePlayer;

            if ((GameBoard[0, 0] == playerSign && GameBoard[0, 1] == playerSign && GameBoard[0, 2] == playerSign)
                || (GameBoard[1, 0] == playerSign && GameBoard[1, 1] == playerSign && GameBoard[1, 2] == playerSign)
                || (GameBoard[2, 0] == playerSign && GameBoard[2, 1] == playerSign && GameBoard[2, 2] == playerSign)
                || (GameBoard[0, 0] == playerSign && GameBoard[1, 0] == playerSign && GameBoard[2, 0] == playerSign)
                || (GameBoard[0, 1] == playerSign && GameBoard[1, 1] == playerSign && GameBoard[2, 1] == playerSign)
                || (GameBoard[0, 2] == playerSign && GameBoard[1, 2] == playerSign && GameBoard[2, 2] == playerSign)
                || (GameBoard[0, 0] == playerSign && GameBoard[1, 1] == playerSign && GameBoard[2, 2] == playerSign)
                || (GameBoard[0, 2] == playerSign && GameBoard[1, 1] == playerSign && GameBoard[2, 0] == playerSign))
            {
                return true;
            }

            return false;
        }
    }
}