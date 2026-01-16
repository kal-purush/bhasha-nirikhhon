using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Drawing;

namespace ChessKing
{
    class Bishop : Chess
    {
        string linkPoint = "Image\\circle.png";
        public Bishop()
		{
			this.isBishop = true;
		}
        public override void FindWay(ChessSquare[,] board, int row, int col)
        {
            //left up
            int j = col - 1;
            for (int i = row - 1; i >= 0; i--)
            {
                if (j < 0) break;
                if (board[i, j].Chess == null)
                {
                    //load blue poin on button, in the way of piece
                    if (Common.IsTurn % 2 == 0 || Common.IsMode == true)
                        board[i, j].Image = Image.FromFile(linkPoint);
                    Common.CanMove.Add(board[i, j]);
                }
                else
                {
                    //square is not empty, check color ,if diffirence about color, change back color
                    if (this.Team != board[i, j].Chess.Team)
                    {
                        if (Common.IsTurn % 2 == 0 || Common.IsMode == true)
                            board[i, j].BackColor = Color.Red;
                        Common.CanMove.Add(board[i, j]);
                        break;
                    }
                    else
                    {
                        break;
                    }
                }
                j--;
            }

            //left down
            j = col - 1;
            for (int i = row + 1; i < 8; i++)
            {
                if (j < 0) break;
                if (board[i, j].Chess == null)
                {
                    //load blue poin on button, in the way of piece
                    if (Common.IsTurn % 2 == 0 || Common.IsMode == true)
                        board[i, j].Image = Image.FromFile(linkPoint);
                    Common.CanMove.Add(board[i, j]);
                }
                else
                {
                    //square is not empty, check color ,if diffirence about color, change back color
                    if (this.Team != board[i, j].Chess.Team)
                    {
                        if (Common.IsTurn % 2 == 0 || Common.IsMode == true)
                             board[i, j].BackColor = Color.Red;
                        Common.CanMove.Add(board[i, j]);
                        break;
                    }
                    else
                    {
                        break;
                    }
                }
                j--;
            }

            //right up
            j = col + 1;
            for (int i = row - 1; i >= 0; i--)
            {
                if (j == 8) break;
                if (board[i, j].Chess == null)
                {
                    //load blue poin on button, in the way of piece
                    if (Common.IsTurn % 2 == 0 || Common.IsMode == true)
                        board[i, j].Image = Image.FromFile(linkPoint);
                    Common.CanMove.Add(board[i, j]);
                }
                else
                {
                    //square is not empty, check color ,if diffirence about color, change back color
                    if (this.Team != board[i, j].Chess.Team)
                    {
                        if (Common.IsTurn % 2 == 0 || Common.IsMode == true)
                            board[i, j].BackColor = Color.Red;
                        Common.CanMove.Add(board[i, j]);
                        break;
                    }
                    else
                    {
                        break;
                    }
                }
                j++;
            }

            //right down
            j = col + 1;
            for (int i = row + 1; i < 8; i++)
            {
                if (j == 8) break;
                if (board[i, j].Chess == null)
                {
                    //load blue poin on button, in the way of piece
                    if (Common.IsTurn % 2 == 0 || Common.IsMode == true)
                        board[i, j].Image = Image.FromFile(linkPoint);
                    Common.CanMove.Add(board[i, j]);
                }
                else
                {
                    //square is not empty, check color ,if diffirence about color, change back color
                    if (this.Team != board[i, j].Chess.Team)
                    {
                        if (Common.IsTurn % 2 == 0 || Common.IsMode == true)
                            board[i, j].BackColor = Color.Red;
                        Common.CanMove.Add(board[i, j]);
                        break;
                    }
                    else
                    {
                        break;
                    }
                }
                j++;
            }
        }
    }
}