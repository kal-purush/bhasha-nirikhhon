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
        private bool showTileCoord = true; // show the tile coordinates
        private bool[,] validMovesGrid; // Store the validity of all possible moves // precalc in order to reduce invalidate/ refresh time 
        private bool showOpponentValidMoves = false; // show the valid moves of the opponent

        private int edge;

        private Size TileSize;
        private Size Offset; // Offset for the board // now is left top corner

        private int noOfTiles = 8; // 8 for the board and 1 for the labels
        private bool debugMode;

        public static Color BlackTilesColor = Color.LimeGreen;
        public static Color WhiteTilesColor = Color.MintCream;
        public static Color BlackPiecesColor = Color.Black;
        public static Color WhitePiecesColor = Color.White;

        public static class PositionColor
        {

            public static Color Path = Color.Turquoise; // possible moves & selected piece
            public static Color Check = Color.Red; // check 
            public static Color NotPath = Color.Salmon; // notPath
            public static Color Special = Color.Violet; // special moves
        }

        private Position selectedPiece;
        private Position pieceMoveTo;

        public PromotionType selectedPromotion { get; set; }

        //========= SETTINGS ========//

        public void DrawBoard(Graphics g, ChessBoard chessBoard, int edgeSet = 800, int offsetX = 75, int offsetY = 75, bool isDebug = false, PieceColor side = null)
        {
           
            if (g == null)
            {
                Debug.WriteLine("Graphics object is null");
            }
            debugMode = isDebug;
            edge = edgeSet;
            TileSize = new Size(edge / noOfTiles, edge / noOfTiles);
            Offset = new Size(offsetX, offsetY);

            try
            {
                // Pre-calculate valid moves and store them in validMovesGrid
                CalculateValidMoves(chessBoard, side);
                // Draw tiles and pieces separately for better performance
                Parallel.Invoke
                    (() => DrawTiles(g, chessBoard, side),
                    () => DrawPieces(g, chessBoard, side)
                    );
            } catch (Exception e)
            {
                Debug.WriteLineIf(isDebug,"Error in DrawBoard: " + e.Message);
            }

        }

        private void DrawTiles(Graphics g, ChessBoard chessBoard, PieceColor side)
        {
            bool white = true;
            Brush b;
            Pen pen = new Pen(new SolidBrush(Color.Black), 0);

            for (int i = 0; i < noOfTiles; i++)
            {
                for (int j = 0; j < noOfTiles; j++)
                {
                    var x = i;
                    var y = j;
                    if (side == PieceColor.Black) // if the side is black, flip the board
                    {
                        x = noOfTiles - 1 - i;
                        y = noOfTiles - 1 - j;
                    }


                    // Use a single brush object for different tile colors
                    //Mark all posibles moves
                    b = white ? new SolidBrush(WhiteTilesColor) : new SolidBrush(BlackTilesColor);
                    pen = new Pen(new SolidBrush(Color.Black), 0);

                    if (selectedPiece != null && (validMovesGrid[x, y] || selectedPiece.X == x && selectedPiece.Y == y))
                    {
                        b = (chessBoard[selectedPiece.toSAN()] != null && chessBoard.Turn == chessBoard[selectedPiece.toSAN()].Color) ? chessBoard.Turn == side || showOpponentValidMoves && chessBoard.Turn != side ? new SolidBrush(PositionColor.Path) : new SolidBrush(PositionColor.NotPath) : new SolidBrush(PositionColor.NotPath);
                        pen = new Pen(new SolidBrush(Color.Black), 2);

                    }

                    // If white kings are in check, mark the tiles
                    if (chessBoard.WhiteKingChecked && chessBoard.WhiteKing.X == x && chessBoard.WhiteKing.Y == noOfTiles - 1 - y)
                    {
                        b = new SolidBrush(PositionColor.Check);
                    }
                    // If black kings are in check, mark the tiles
                    if (chessBoard.BlackKingChecked && chessBoard.BlackKing.X == x && chessBoard.BlackKing.Y == noOfTiles - 1 - y)
                    {
                        b = new SolidBrush(PositionColor.Check);
                    }

                    g.FillRectangle(b, i * TileSize.Width + Offset.Width, j * TileSize.Height + Offset.Height, TileSize.Width, TileSize.Height);
                    g.DrawRectangle(pen, i * TileSize.Width + Offset.Width, j * TileSize.Height + Offset.Height, TileSize.Width, TileSize.Height);



                    if (showTileCoord)
                    {
                        g.DrawString((char)('a' + x) + (noOfTiles - y).ToString(), new Font("Arial", 10), new SolidBrush(Color.Red), i * TileSize.Width + Offset.Width + 5, j * TileSize.Height + Offset.Height + 5);
                    }

                    if (j == noOfTiles - 1)
                    {
                        g.DrawString(((char)('a' + x)).ToString(), new Font("Arial", 13), new SolidBrush(Color.Red), i * TileSize.Width + Offset.Width + TileSize.Width - (float)(TileSize.Width * .7), j * TileSize.Height + Offset.Height + TileSize.Height + (float)(TileSize.Height * .2));
                    }
                    if (i == 0)
                    {
                        g.DrawString((noOfTiles - y).ToString(), new Font("Arial", 13), new SolidBrush(Color.Red), i * TileSize.Width + Offset.Width - (float)(TileSize.Width * .5), j * TileSize.Height + Offset.Height + (float)(TileSize.Height * .2));
                    }



                    // Dispose the brush after use
                    b.Dispose();

                    white = !white;
                }
                white = !white;
            }
        }

        private void DrawPieces(Graphics g, ChessBoard chessBoard, PieceColor side)
        {
            Bitmap bitmap;

            for (int i = 0; i < noOfTiles; i++)
            {
                for (int j = 0; j < noOfTiles; j++)
                {
                    // Retrieve the piece once instead of multiple times
                    var x = i;
                    var y = j;
                    if (side == PieceColor.Black) // if the side is black, flip the board
                    {
                        x = noOfTiles - 1 - i;
                        y = noOfTiles - 1 - j;
                    }

                    var piece = chessBoard[new Position(x, y).toSAN()];
                    if (piece != null)
                    {
                        bitmap = new PiecesCustomize().mapFromSANToBitmap(g, piece.ToString()[1], piece.ToString()[0] == 'w');
                        if (bitmap != null)
                        {
                            g.DrawImage(bitmap, i * TileSize.Width + Offset.Width, j * TileSize.Height + Offset.Height, TileSize.Width, TileSize.Height);
                            bitmap.Dispose(); // Dispose the bitmap after use
                        }
                    }
                }
            }
        }
        private void CalculateValidMoves(ChessBoard chessBoard, PieceColor side)
        {
            validMovesGrid = new bool[noOfTiles, noOfTiles];

            // Iterate over all possible positions and check their validity
            for (int i = 0; i < noOfTiles; i++)
            {
                for (int j = 0; j < noOfTiles; j++)
                {
                    Position position = new Position(i, j);

                    // Check if a piece is selected and it's a valid move
                    if (selectedPiece != null && IsValidMove(selectedPiece, position, chessBoard))
                    {
                        if (chessBoard[selectedPiece.toSAN()].Color != side && !showOpponentValidMoves)
                        {
                            validMovesGrid[i, j] = false;
                            continue;
                        }
                        validMovesGrid[i, j] = true;
                    }
                }
            }
        }

        public static bool IsInBoard(Position p) // check for click if out of the board or not
        {
            return p.X >= 0 && p.Y >= 0 && p.X <= 7 && p.Y <= 7;
        }

        public bool checkForEndgame(ChessBoard chessBoard)
        {
            // Check if the game has ended
            if (chessBoard.IsEndGame)
            {
                ResetSelection();
                if (chessBoard.EndGame.WonSide != null)
                    Debug.WriteLineIf(debugMode, "The game has ended. " + chessBoard.EndGame.WonSide + " won!");
                else
                    Debug.WriteLineIf(debugMode, "The game has ended. It's a draw!");

                return true;
            }
            return false;
        }

        public ChessBoard onClicked(Position clickedPosition, ChessBoard chessBoard, bool isNormalized = false, PieceColor side = null)
        {

            if (checkForEndgame(chessBoard) == true) return chessBoard; // check if the game has ended


            // Normalize the clicked position if needed
            if (isNormalized == false)
            {
                clickedPosition = NormalizePosition(clickedPosition, side);
                Debug.WriteLineIf(debugMode, "Normalized position: " + clickedPosition.toSAN());
            }

            // Check if the clicked position is within the board range from (0,0) to (7,7)
            if (!IsInBoard(clickedPosition))
            {
                Debug.WriteLineIf(debugMode, "Clicked position is out of board");
                ResetSelection();
                return chessBoard;
            }

            // If no piece is selected, try to select one
            if (selectedPiece == null)
            {
                if (!SelectPiece(clickedPosition, chessBoard))
                {
                    Debug.WriteLineIf(debugMode, "No piece on clicked position");
                    ResetSelection();
                    return chessBoard;
                }
            }
            else
            {
                // If a piece is already selected, move to the clicked position
                pieceMoveTo = clickedPosition;

                // Change selected piece if the clicked position has a piece in same side or a piece is not be taken by current selected piece
                if (chessBoard[pieceMoveTo.toSAN()] != null && !IsValidMove(selectedPiece, pieceMoveTo, chessBoard))
                {
                    Debug.WriteLineIf(debugMode, "Change select piece on " + chessBoard[pieceMoveTo.toSAN()]);
                    selectedPiece = pieceMoveTo;
                    pieceMoveTo = null;
                    return chessBoard;
                }

            }

            // Put this turn check here for click effect still active
            if (chessBoard.Turn != side) return chessBoard; // check if it's the right turn 

            // If no position is selected to move to, return
            if (pieceMoveTo == null)
            {
                return chessBoard;
            }

            // If clicked on the same piece, deselect it
            if (pieceMoveTo.Equals(selectedPiece))
            {
                Debug.WriteLineIf(debugMode, "Clicked on the same piece");
                ResetSelection();
                return chessBoard;
            }

            // Validate the move
            if (!IsValidMove(selectedPiece, pieceMoveTo, chessBoard))
            {
                Debug.WriteLineIf(debugMode, "Invalid Move: " + selectedPiece.toSAN() + " to " + pieceMoveTo.toSAN());
                ResetSelection();
                return chessBoard;
            }

            // Check for pawn promotion
            if (chessBoard[selectedPiece.toSAN()].Type == PieceType.Pawn && (pieceMoveTo.Y == 0 || pieceMoveTo.Y == 7))
            {
                Debug.WriteLineIf(debugMode, "Pawn promotion");
                selectedPromotion = new SpawnServerAndClient().PromotePawnUIAsync();
            }

            // Execute the move
            chessBoard.Move(new Move(selectedPiece.toSAN(), pieceMoveTo.toSAN()));
            Debug.WriteLineIf(debugMode, "Moved piece from " + selectedPiece.toSAN() + " to " + pieceMoveTo.toSAN());
            Debug.WriteLineIf(debugMode, chessBoard.ToAscii());


            ResetSelection();

            return chessBoard;
        }


        private void ResetSelection()
        {
            selectedPiece = null;
            pieceMoveTo = null;
        }

        private Position NormalizePosition(Position position, PieceColor side)
        {
            var x = (position.X - Offset.Width) / TileSize.Width;
            var y = (position.Y - Offset.Height) / TileSize.Height;
            if (side == PieceColor.Black) // if the side is black, flip the board
            {
                x = noOfTiles - 1 - x;
                y = noOfTiles - 1 - y;
            }
            return new Position(x, y);
        }

        private bool SelectPiece(Position clickedPosition, ChessBoard chessBoard)
        {
            // Check if there's a piece at the clicked position
            if (chessBoard[clickedPosition.toSAN()] == null)
            {
                return false;
            }

            selectedPiece = clickedPosition;
            Debug.WriteLineIf(debugMode, "Selected piece on " + chessBoard[selectedPiece.toSAN()]);
            return true;
        }

        private bool IsValidMove(Position fromPosition, Position toPosition, ChessBoard chessBoard)
        {
            try
            {// Debug.WriteLineIf(debugMode && !chessBoard.IsValidMove(new Move(fromPosition.toSAN(), toPosition.toSAN())), "Invalid move: " + fromPosition.toSAN() + " to " + toPosition.toSAN());
                return chessBoard.IsValidMove(new Move(fromPosition.toSAN(), toPosition.toSAN()));
            }
            catch (Exception e)
            {
                Debug.WriteLineIf(debugMode, "Error in IsValidMove: " + e.Message);
                return false;
            }
        }

    }
}