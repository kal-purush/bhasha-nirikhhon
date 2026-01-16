using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace TicTacToe_Minimax
{
    class Board
    {
        public enum State { BLANK, X, O };

        List<int> availablePoints;

        private State[][] board;
        private State playersTurn;
        private State winner;

        private int moveCount;
        private bool gameOver;
        public Board()
        {
            board = new State[3][];
            availablePoints = new List<int>();
            reset();
        }
        public void reset()
        {
            moveCount = 0;
            gameOver = false;
            playersTurn = State.X;
            winner = State.BLANK;
            initialize();
        }

        private void initialize()
        {
            for (int row = 0; row < 3; row++)
            {
                State[] c = new State[3];
                for (int col = 0; col < 3; col++)
                {
                    c[col] = State.BLANK;
                }
                board[row] = c;
            }
            availablePoints.Clear();
            for (int i = 0; i < 9; i++)
            {
                availablePoints.Add(i);
            }
        }
        public int getMoveCount()
        {
            return moveCount;
        }
        //heuristic value calculated according to:
        //(number of available rows,columns and diagonals for player)-(number of available rows,columns and diagonals for computer)
        public int heuristic()
        {
            Board.State player = Board.State.X;
            Board.State opponent = Board.State.O;
            return (heuristics(player)) - (heuristics(opponent));
        }
        //gets the number of available rows, columns and diagonals for specified State.
        private int heuristics(Board.State player)
        {
            Board.State opponent = (player == Board.State.X) ? Board.State.O : Board.State.X;

            int heuristic = 0;
            for (int x = 0; x < 3; x++)
            {
                //for each row
                for (int i = 0; i < 3; i++)
                {
                    if (board[x][i] == opponent)
                    {
                        break;
                    }
                    if (i == 2)
                    {
                        heuristic++;
                    }
                }
                //for each column
                for (int i = 0; i < 3; i++)
                {
                    if (board[i][x] == opponent)
                    {
                        break;
                    }
                    if (i == 2)
                    {
                        heuristic++;
                    }
                }
            }
            //diagonal
            for (int i = 0; i < 3; i++)
            {
                if (board[i][i] == opponent)
                {
                    break;
                }
                if (i == 2)
                {
                    heuristic++;
                }
            }
            //diagonal
            for (int i = 0; i < 3; i++)
            {
                if (board[2 - i][i] == opponent)
                {
                    break;
                }
                if (i == 2)
                {
                    heuristic++;
                }
            }
            return heuristic;
        }
        public State getWinner()
        {
            return winner;
        }
        public State getTurn()
        {
            return playersTurn;
        }
        public List<int> getAvailableStates()
        {
            return availablePoints;
        }
        public void move(int index)
        {
            int x = index / 3;
            int y = index % 3;

            if (board[x][y] == State.BLANK)
            {
                board[x][y] = playersTurn;
            }

            moveCount++;
            availablePoints.Remove(x * 3 + y);

            // The game is a draw.
            if (moveCount == 9)
            {
                winner = State.BLANK;
                gameOver = true;
            }

            checkRow(x);
            checkColumn(y);
            checkDiagonalFromTopLeft(x, y);
            checkDiagonalFromTopRight(x, y);

            playersTurn = (playersTurn == State.X) ? State.O : State.X;
        }
        private void checkRow(int row)
        {
            for (int i = 1; i < 3; i++)
            {
                if (board[row][i] != board[row][i - 1])
                {
                    break;
                }
                if (i == 2)
                {
                    winner = playersTurn;
                    gameOver = true;
                }
            }
        }
        private void checkColumn(int column)
        {
            for (int i = 1; i < 3; i++)
            {
                if (board[i][column] != board[i - 1][column])
                {
                    break;
                }
                if (i == 2)
                {
                    winner = playersTurn;
                    gameOver = true;
                }
            }
        }
        private void checkDiagonalFromTopLeft(int x, int y)
        {
            if (x == y) //if on the diagonal line
            {
                for (int i = 1; i < 3; i++)
                {
                    if (board[i][i] != board[i - 1][i - 1])
                    {
                        break;
                    }
                    if (i == 2)
                    {
                        winner = playersTurn;
                        gameOver = true;
                    }
                }
            }
        }
        private void checkDiagonalFromTopRight(int x, int y)
        {
            if (2 - x == y)
            {
                for (int i = 1; i < 3; i++)
                {
                    if (board[2 - i][i] != board[3 - i][i - 1])
                    {
                        break;
                    }
                    if (i == 2)
                    {
                        winner = playersTurn;
                        gameOver = true;
                    }
                }
            }
        }
        public bool isGameOver()
        {
            return gameOver;
        }
        //creates a deep copy of the board.
        public Board getDeepCopy()
        {
            Board board = new Board();

            for (int i = 0; i < board.board.Length; i++)
            { 
                board.board[i] = (State[])this.board[i].Clone();
            }
            board.playersTurn = this.playersTurn;
            board.winner = this.winner;
            board.availablePoints = new List<int>();
            board.availablePoints.AddRange(this.availablePoints);
            board.moveCount = this.moveCount;
            board.gameOver = this.gameOver;
            return board;
        }
        //get states with State O.
        public List<int> oStates()
        {
            List<int> oStates = new List<int>();
            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                {
                    if (board[i][j] == State.O)
                    {
                        oStates.Add(i*3+j);
                    }
                }
            }
            return oStates;
        }
        //Puts a random O to the board, called if the first turn is computer's.
        public int playComputerTurn()
        {
            Random rand = new Random();
            int index = rand.Next(9);
            playersTurn = Board.State.O;

            move(index);
            return index;
        }
        //***************************************************************************//
        public void debug()
        {
            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                {
                    Debug.Write(board[i][j] + " ");
                }
                Debug.WriteLine("");
            }
        }
        //used it for debugging purposes.
        public static void randomBoard(Board board)
        {
            Random rand = new Random();
            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                {
                    Array values = Enum.GetValues(typeof(State));
                    State randomBar = (State)values.GetValue(rand.Next(values.Length));
                    board.board[i][j] = randomBar;
                }
            }
        }
    }
}