using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrueChessGame
{
    public class ChessBoard
    {
        private byte height;

        private sbyte[,] squares;

        public byte Height
        {
            get
            {
                return height;
            }
        }

        public sbyte this[char file, int rank]
        {
            get
            {
                return squares[file - 97, rank-1];
            }
            set
            {
                squares[file - 97, rank-1] = value;
            }
        }

        public ChessBoard()
        {
            height = 8;
            squares = new sbyte[height, height];

        }

        public ChessBoard(byte heightToSet)
        {
            if (heightToSet > 26)
            {
                height = 26;
            }
            else
            {
                height = heightToSet;
            }
            squares = new sbyte[height, height];
        }

        public ChessBoard(ChessBoard other)
        {
            this.height = other.height;
            this.squares = (sbyte[,]) other.squares.Clone();
        }

        public ChessBoard ShallowCopy()
        {
            return new ChessBoard(this);
        }

        public void SetDefaultChessBoard()
        {
            //            0 - empty
            //>0 - white
            //<0 - black
            //1 - p - pawn 
            //2 - N - kNight
            //3 - B - Bishop
            //4 - R - Rook
            //5 - Q - Queen
            //6 - K - King
            //first White
            //Console.WriteLine('a' - 97);
            this['a', 1] = 4;
            this['b', 1] = 2;
            this['c', 1] = 3;
            this['d', 1] = 5;
            this['e', 1] = 6;
            this['f', 1] = 3;
            this['g', 1] = 2;
            this['h', 1] = 4;
            for (char file = 'a'; file <= 'h'; file++)
            {
                this[file, 2] = 1;
            }
            //now Black
            this['a', 8] = -4;
            this['b', 8] = -2;
            this['c', 8] = -3;
            this['d', 8] = -5;
            this['e', 8] = -6;
            this['f', 8] = -3;
            this['g', 8] = -2;
            this['h', 8] = -4;
            for (char file = 'a'; file <= 'h'; file++)
            {
                this[file, 7] = -1;
            }
        }

        public void DebugConsoleSimpleDraw()
        {
            for (int trank=8; trank>=1; trank--)
            {
                Console.WriteLine();
                for (char tfile='a'; tfile<='h'; tfile++)
                {
                    //Console.WriteLine(tfile + " " + trank);
                    if (this[tfile, trank] < 0)
                    {
                        Console.Write(this[tfile, trank] + " ");
                    }
                    else if (this[tfile, trank]>0)
                    {
                        Console.Write("+" + this[tfile, trank]+" ");
                    }
                    else
                    {
                        Console.Write(" 0 ");
                    }
                }
            }
        }
    }
}