using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Chess;
using System.Diagnostics;

namespace ChessAI
{
    internal class BoardRenderer
    {
        //========= SETTINGS ========//
        private bool WhiteTurn = true;

        private int edge;

        private Size TileSize;
        private Size Offset; // Offset for the board // now is left top corner

        private int noOfTiles = 8; // 8 for the board and 1 for the labels
        private bool debugMode;

        public static Color BlackTilesColor = Color.Green;
        public static Color WhiteTilesColor = Color.White;
        public static Color BlackPiecesColor = Color.Black;
        public static Color WhitePiecesColor = Color.White;

        public static class PositionColor
        {
            public static Color Path = Color.FromArgb(50, 0, 255, 0); // selected & possible moves
            public static Color Check = Color.FromArgb(50, 255, 0, 0); // check
            public static Color Special = Color.FromArgb(50, 0, 0, 255); // special moves
        }

        private Position selectedPiece;
        private Position pieceMoveTo;
        
        
        //========= SETTINGS ========//

        public void DrawBoard(Graphics g, ChessBoard chessBoard, int edge_set = 800, int OffsetX = 75, int OffsetY = 75,bool isDebug = false) // Begin the game 
        {
            debugMode = isDebug; // default = false
            edge = edge_set; //edge_set is the size of the board //default 800
            TileSize = new Size(edge/noOfTiles, edge/noOfTiles);
            Offset = new Size(OffsetX, OffsetY);
            DrawTiles(g);
            DrawPieces(g, chessBoard);
        }
        private void DrawTiles(Graphics g)
        {
            bool White = true;
            Brush b;
            for (int i = 0; i < noOfTiles ; i++)
            {
                for (int j = 0; j < noOfTiles ; j++)
                {
                    b = new SolidBrush(White ? WhiteTilesColor: BlackTilesColor);
                    
                    g.FillRectangle(b, i * TileSize.Width + Offset.Width, j * TileSize.Height + Offset.Height, TileSize.Width, TileSize.Height);
                    g.DrawRectangle(new Pen(new SolidBrush(Color.Black), 2), i * TileSize.Width + Offset.Width, j * TileSize.Height + Offset.Height, TileSize.Width, TileSize.Height);

                    if (debugMode)
                    {
                        g.DrawString((char)('a' + i) + (noOfTiles - j).ToString(), new Font("Arial", 10), new SolidBrush(Color.Red), i * TileSize.Width + Offset.Width + 5, j * TileSize.Height + Offset.Height + 5);
                    }
                    else
                    {
                        if(j == noOfTiles - 1)
                        {
                            g.DrawString(((char)('a' + i)).ToString(), new Font("Arial", 13), new SolidBrush(Color.Red), i * TileSize.Width + Offset.Width  + TileSize.Width - (float)(TileSize.Width*.4) , j * TileSize.Height + Offset.Height + TileSize.Height + (float)(TileSize.Height * .2));
                        }
                        if(i == 0) {
                            g.DrawString((noOfTiles - j).ToString(), new Font("Arial", 13), new SolidBrush(Color.Red), i * TileSize.Width + Offset.Width - (float)(TileSize.Width * .5), j * TileSize.Height + Offset.Height + (float)(TileSize.Height * .2));
                        }
                    }

                    b.Dispose();
                    White = !White;
                }
                White = !White;
            }
        }
        private void DrawPieces(Graphics g, ChessBoard chessBoard)
        {
            Bitmap bitmap;
            for (int i = 0; i < noOfTiles; i++)
            {
                for (int j = 0; j < noOfTiles; j++)
                {
                    bitmap = (chessBoard[new Position(i, j).toSAN()]!= null)? new PiecesCustomize().mapFromSANToBitmap(g, chessBoard[new Position(i, j).toSAN()].ToString()[1], chessBoard[new Position(i, j).toSAN()].ToString()[0] == 'w') : null;
                    if (bitmap == null) continue; // skip if there is no piece
                    g.DrawImage(bitmap, i * TileSize.Width + Offset.Width, j * TileSize.Height + Offset.Height, TileSize.Width, TileSize.Height);
                }
            }

        }

        public static bool IsInBoard(Position p) // check for click if out of the board or not
        {
            return p.X >= 0 && p.Y >= 0 && p.X <= 7 && p.Y <= 7;
        }

        public ChessBoard onClicked(Position p , ChessBoard chessBoard, bool isNormalize = false)
        {
            if (chessBoard.IsEndGame)
            {
                Debug.WriteLineIf(debugMode, "Game is ended: " + chessBoard.EndGame.WonSide + " won!");
                return chessBoard; // if game is ended
            }; 

            Position clickedPosition = new Position((p.X - Offset.Width) / TileSize.Width, (p.Y - Offset.Height) / TileSize.Height);
            if (isNormalize) // the p has been normalized in range (0,0) to (7,7)
            {
                clickedPosition = p;
            }

                if (!IsInBoard(clickedPosition))
            {
                Debug.WriteLineIf(debugMode,"Clicked out of board");
                selectedPiece = null;
                pieceMoveTo = null; // deselect all
                return chessBoard;
            }
            //clickedPosition.setColor(PositionColor.Path);
            //Debug.WriteLine("Clicked on: " + clickedPosition.X +" "+clickedPosition.Y);
           // Debug.WriteLine("Clicked on: " + clickedPosition.toSAN());

            if (selectedPiece == null) 
            {
                // check if have piece on clicked position
                if (chessBoard[clickedPosition.toSAN()] == null) // if no piece on clicked position
                {
                    Debug.WriteLineIf(debugMode, "No piece on clicked position");
                    return chessBoard;
                } 
                selectedPiece = clickedPosition; // if not selected any piece set to selected
                Debug.WriteLineIf(debugMode, "Selected piece on " + chessBoard[selectedPiece.toSAN()]);
            }
            else
            {
                pieceMoveTo = clickedPosition; // if not selected any piece set to move to
            }
            
            if(pieceMoveTo == null) return chessBoard; // if not selected any position to move to
            
            if (pieceMoveTo.Equals(selectedPiece))  // if clicked on the same piece
            {
                Debug.WriteLineIf(debugMode, "Clicked on the same piece");
                selectedPiece = null;
                pieceMoveTo = null; // deselect all
                return chessBoard;
            }

            if (!chessBoard.IsValidMove(new Move(selectedPiece.toSAN(), pieceMoveTo.toSAN())) ) { 
                Debug.WriteLineIf(debugMode, "Invalid Move: " + selectedPiece.toSAN() + " moveto " + pieceMoveTo.toSAN()); // if move is invalid
                selectedPiece = null;
                pieceMoveTo = null; // deselect all
                return chessBoard; 
            }

            Debug.WriteLineIf(debugMode, "Pieces on " + selectedPiece.toSAN() + " moveto " + pieceMoveTo.toSAN());

            chessBoard.Move(new Move(selectedPiece.toSAN(),pieceMoveTo.toSAN()));
            Debug.WriteLineIf(debugMode,chessBoard.ToAscii());

            //move complete
            selectedPiece = null;
            pieceMoveTo = null; // deselect all

            return chessBoard;
        }
    }
}