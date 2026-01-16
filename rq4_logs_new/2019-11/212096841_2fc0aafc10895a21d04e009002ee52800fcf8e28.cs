using FramerateStabilizer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Tetfuza.Interfaces;

namespace Tetfuza
{
    public class TetfuzaBoard
    {

        public const int BOARD_HEIGHT = 22;
        public const int BOARD_WIDTH = 10;
        public const char LOCKDOWN_CHAR = '0';
        public const char MOVING_CHAR = '@';
        public const char EMPTY_CHAR = ' ';
        public const char TOPOUT_CHAR = '%';

        public List<List<char>> Board { get; private set; }

        private List<char> EmptyLine
        {
            get
            {
                return BoardLine(EMPTY_CHAR);
            }
        }
        /// <summary>
        /// Create a new board with {BOARD_HEIGHT} rows, and {BOARD_WIDTH} columns, all filled with ' ' chars
        /// </summary>
        public TetfuzaBoard()
        {
            Board = new List<List<char>>();
            for (int height = 0; height < BOARD_HEIGHT; height++)
            {
                Board.Add(EmptyLine);
            }
        }
        

        private List<char> BoardLine(char fillerChar)
        {
            var line = new List<char>();
            for (int i = 0; i < BOARD_WIDTH; i++)
            {
                line.Add(fillerChar);
            }
            return line;
        }

        public override string ToString()
        {
            string boardString = "";
            for (int row = 2; row < Board.Count; row++)
            {
                for (int col = 0; col < BOARD_WIDTH; col++)
                {
                    boardString += Board[row][col] + " ";
                }
                boardString += ")";
            }

            return boardString;
        }

        /// <summary>
        /// Takes data from CurrentPiece.Piece (a 2-D List that represents piece orientation), 
        /// and copies it to the Board, centered around member variable "_pieceCenter"
        /// </summary>
        /// <param name="fuzaChar">The character to write to the Board</param>
        public void DrawPiece(FuzaPiece currentPiece, Coordinate pieceCenter, char fuzaChar = MOVING_CHAR)
        {
            for (int row = 0; row < FuzaPiece.PIECE_SIZE; row++)
            {
                for (int col = 0; col < FuzaPiece.PIECE_SIZE; col++)
                {
                    char fuza = currentPiece.Piece[row][col];
                    int colCoord = pieceCenter.xPos - (FuzaPiece.PIECE_SIZE / 2) + col;
                    int rowCoord = pieceCenter.yPos - (FuzaPiece.PIECE_SIZE / 2) + row;
                    if (colCoord >= 0 && colCoord < BOARD_WIDTH
                        && rowCoord >= 0 && rowCoord < BOARD_HEIGHT)
                    {
                        if (fuza != FuzaPiece.BLANK_CHAR)
                        {
                            Board[rowCoord][colCoord] = fuzaChar;
                        }
                        else if (fuza == FuzaPiece.BLANK_CHAR && Board[rowCoord][colCoord] != LOCKDOWN_CHAR)
                        {
                            Board[rowCoord][colCoord] = EMPTY_CHAR;
                        }
                    }
                }
            }
        }
        /// <summary>
        /// Draws the TOPOUTCHAR over the board one line at a time, starting at the top
        /// </summary>
        public void TopOutAnimation(int msPerFrame, Stopwatch timer, IDisplay screen)
        {
            for (int i = 0; i < BOARD_HEIGHT; i++)
            {
                StableFrames.Stabilize(msPerFrame * 5, timer);
                Board[i] = BoardLine(TOPOUT_CHAR);
                screen.DrawBoard(ToString());
            }

            var gameO1 = new List<char>();
            gameO1.AddRange(BoardLine(TOPOUT_CHAR));
            gameO1.InsertRange(3, "GAME");
            Board[BOARD_HEIGHT / 2] = gameO1;

            var gameO2 = new List<char>();
            gameO2.AddRange(BoardLine(TOPOUT_CHAR));
            gameO2.InsertRange(3, "OVER");
            Board[BOARD_HEIGHT / 2 + 1] = gameO2;

            StableFrames.Stabilize(msPerFrame * 4, timer);
            screen.DrawBoard(ToString());
        }

        public int ClearLines(int msPerFrame, Stopwatch timer, IDisplay screen)
        {
            int linesCleared = 0;
            List<int> clearedIndex = new List<int>();
            for (int i = 0; i < BOARD_HEIGHT; i++)
            {
                if (!(Board[i].Contains(EMPTY_CHAR) || Board[i].Contains(MOVING_CHAR)))
                {
                    linesCleared++;
                    Board[i] = BoardLine('-');
                    clearedIndex.Add(i);
                }
            }
            // Pause for clear animation (currently 10 frames)
            screen.DrawBoard(ToString());
            StableFrames.Stabilize(msPerFrame * 10, timer);
            for (int index = 0; index < linesCleared; index++)
            {
                Board.RemoveAt(clearedIndex[index]);
                Board.Insert(0, EmptyLine);
            }
            return linesCleared;
        }
    }
}