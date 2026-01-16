using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TicTacToeShared;

namespace TicTacToeBossEngine
{
    /// <summary>
    /// This player randomly selects an available move to play.
    /// </summary> 
    public class BossEngine2 : IPlayerEngine
    {
        
        private static Random rnd = new Random();
        public void BeforeGame()
        {
        }
        public void AfterGame(int player, Board state, List<Move> moves) { }


        public Move GetMove(int player, Board state, List<Move> moves)
        {

            var db = BoardDatabase.Current();

            var availMoves = state.GetAvailableMoves();

            //loop through moves , for each move, play 100 games with random moves. 
            //find the move which result in the MOST winning games.
            var maxWins = 0;
            var maxMove = availMoves[0];
            //var winCount = 0;

            var NumberOfMoves = moves.Count + 1;

            var movesCount = new Dictionary<Move, int>();
            foreach (var move in availMoves)
            {
                movesCount.Add(move, 0);
            }
             
            Parallel.ForEach<Move>(availMoves, (move) =>
            {
                int simPlayer;
                var clonedState = state.Clone();
                //voer deze move , voor deze player vast uit. 
                clonedState.ExecuteMove(move, player);

                //draai de boel vast om.
                simPlayer = (player == 1 ? 2 : 1);

                var fg = FastGame.fromBoard(clonedState, simPlayer);

                // DIT GETAL GEEFT DE STRENGTH AAN .. STOND OP 50. 
                for (int i = 0; i < 2500; i++)
                {
                    if (fg.PlayOneGame() == player)
                    {
                        movesCount[move]++;
                    }
                }

            });


            //selecteer de hoogste move

            foreach (var move in movesCount)
            {
                if (move.Value > maxWins)
                {
                    maxWins = move.Value;
                    maxMove = move.Key;
                }
            }


            return maxMove;
        }
    }



    /// <summary>
    /// This player randomly selects an available move to play.
    /// </summary> 
    public class BossEngine : IPlayerEngine
    {
        private static String inspector = "Nee Floris, niet stiekem reflector gebruiken he!";
        private static Random rnd = new Random();
        public void BeforeGame()
        {
            var x = inspector;
        } 
        public void AfterGame(int player, Board state, List<Move> moves) { }


        public Move GetMove(int player, Board state, List<Move> moves)
        {

            var db = BoardDatabase.Current();

            var availMoves = state.GetAvailableMoves();

            //loop through moves , for each move, play 100 games with random moves. 
            //find the move which result in the MOST winning games.
            var maxWins = 0;
            var maxMove = availMoves[0];
            //var winCount = 0;

            var NumberOfMoves = moves.Count + 1;
            
            var movesCount = new Dictionary<Move, int>();
            foreach (var move in availMoves)
            {
                movesCount.Add(move, 0);
            }

            Parallel.ForEach<Move>(availMoves, (move) =>
            {
                int simPlayer;
                var clonedState = state.Clone();
                //voer deze move , voor deze player vast uit. 
                clonedState.ExecuteMove(move, player);

                //draai de boel vast om.
                simPlayer = (player == 1 ? 2 : 1);

                var fg = FastGame.fromBoard(clonedState, simPlayer);
                
                // DIT GETAL GEEFT DE STRENGTH AAN .. STOND OP 50. 
                for (int i = 0; i < 50; i++)
                {  
                    if (fg.PlayOneGame() == player)
                    {
                        movesCount[move]++;
                    }
                }

            });


            //selecteer de hoogste move

            foreach (var move in movesCount)
            {
                if (move.Value > maxWins)
                {
                    maxWins = move.Value;
                    maxMove = move.Key;
                }
            }


            return maxMove;
        }
    }



    public class FastGame
    {
        private BoardDatabase db = BoardDatabase.Current();

        private Random rnd = new Random(DateTime.Now.Millisecond);

        //De smallboard referenties, zoals bij initialisatie van de FastGame
        //Hierdoor kan de fastgame 'snel' weer opnieuw beginnen

        public BoardRecord[,] InitialBoards;

        public int InitialNextColumn;
        public int InitialNextRow;
        public int InitialPlayer;
        BoardRecord[,] Boards;



        public BoardRecord GameBoard;


        private FastGame()
        {
            InitialBoards = new BoardRecord[3, 3];
            Boards = new BoardRecord[3, 3];
        }


        public int PlayOneGame()
        {

            int rndindex;
            CoordWithIndex nextstate;
            Coord coord;
            BoardRecord sb;

            //local vars for speedup ?
            int NextColumn = InitialNextColumn;
            int NextRow = InitialNextRow;
            int Player = InitialPlayer;

            //kopier initial instellingen
            CopyFromInitial();


            GameBoard = db.Data[BoardRecord.BoardsToInt(Boards)];

            while (GameBoard.Winner == 0)
            {
                //pick random smallboard
                if (NextColumn == -1)
                {
                    rndindex = rnd.Next(GameBoard.FreeCoords.Count);
                    coord = GameBoard.FreeCoords[rndindex];
                    NextColumn = coord.Column;
                    NextRow = coord.Row;
                }

                sb = Boards[NextColumn, NextRow];

                //depending on player
                if (Player == 1)
                {
                    rndindex = rnd.Next(sb.NextStates1.Count);
                    nextstate = sb.NextStates1[rndindex];

                }
                else 
                {
                    rndindex = rnd.Next(sb.NextStates2.Count);
                    nextstate = sb.NextStates2[rndindex];
                }
                 
                Boards[NextColumn, NextRow] = db.Data[nextstate.Index];
                NextColumn = nextstate.Column;
                NextRow = nextstate.Row;

                //update het board met de smallboard records
                GameBoard = db.Data[BoardRecord.BoardsToInt(Boards)];

                //check of nextColumn / nextRow naar een plek verwijst die al bezet of vol is.
                if (GameBoard.Cells[NextColumn, NextRow] != 0)
                {
                    NextColumn = -1;
                    NextRow = -1;
                }
                Player = (Player == 1 ? 2 : 1);


            }
            return GameBoard.Winner;


        }

        private void CopyFromInitial()
        {
            //NextColumn = InitialNextColumn;
            //NextRow = InitialNextRow;
            //Player = InitialPlayer;
            for (int col = 0; col < 3; col++)
            {
                for (int row = 0; row < 3; row++)
                {
                    Boards[col, row] = InitialBoards[col, row];
                }
            }
        }

        public static FastGame fromBoard(Board b, int aPlayer)
        {
            int index;
            FastGame result = new FastGame();
            result.InitialNextColumn = b.NextBoardColumn;
            result.InitialNextRow = b.NextBoardRow;
            result.InitialPlayer = aPlayer;


            for (var bc = 0; bc < 3; bc++)
            {
                for (var br = 0; br < 3; br++)
                {

                    index = BoardRecord.CellsToInt(b.SmallBoards[bc, br].GetCells);
                    result.InitialBoards[bc, br] = result.db.Data[index];

                }
            }
            return result;
        }
    }

   

    public class BoardDatabase
    {

        public Dictionary<int, BoardRecord> Data;
        private static BoardDatabase instance;


        private BoardDatabase()
        {
            Data = new Dictionary<int, BoardRecord>();
            BuildData();
        }

        private void BuildData()
        {
            int count = (int)Math.Pow(4, 9) - 1;
            for (int i = 0; i < count; i++)
            {
                Data.Add(i, new BoardRecord(i));
            }

            int index = 0;
            int[,] cellsmove;
            Coord coord;
            BoardRecord b;

            // alle transities toevoegen die mogelijk zijn per speler per bord
            for (int i = 0; i < count; i++)
            {
                b = Data[i];

                for (int f = 0; f < b.FreeCoords.Count; f++)
                {
                    coord = b.FreeCoords[f];
                    //"zet" de move.
                    cellsmove = CopyInts(b.Cells);
                    cellsmove[coord.Column, coord.Row] = 1;
                    index = BoardRecord.IntsToInt(cellsmove);
                    b.NextStates1.Add(new CoordWithIndex() { Column = coord.Column, Row = coord.Row, Index = index });

                    cellsmove[coord.Column, coord.Row] = 2;
                    index = BoardRecord.IntsToInt(cellsmove);
                    b.NextStates2.Add(new CoordWithIndex() { Column = coord.Column, Row = coord.Row, Index = index });
                }
            }
        }

        private Cell[,] CopyCells(Cell[,] input)
        {
            var result = new Cell[3, 3];
            for (int c = 0; c < 3; c++)
            {
                for (int r = 0; r < 3; r++)
                {
                    result[c, r] = input[c, r].Clone();
                }
            }
            return result;
        }

        private int[,] CopyInts(int[,] input)
        {
            var result = new int[3, 3];
            for (int c = 0; c < 3; c++)
            {
                for (int r = 0; r < 3; r++)
                {
                    result[c, r] = input[c, r];
                }
            }
            return result;
        }

        public static BoardDatabase Current()
        {
            if (instance == null)
            {
                instance = new BoardDatabase();
            }
            return instance;
        }

        public BoardRecord fromSmallboard(SmallBoard sb)
        {
            //
            return Data[BoardRecord.CellsToInt(sb.GetCells)];

        }

    }


    public struct Coord
    {
        public int Column;
        public int Row;
    }

    public struct CoordWithIndex
    {
        public int Column;
        public int Row;
        public int Index;
    }

    public class BoardRecord
    {
        public int Index;
        public int[,] Cells;
        public int Winner;

        public List<CoordWithIndex> NextStates1 = new List<CoordWithIndex>();
        public List<CoordWithIndex> NextStates2 = new List<CoordWithIndex>();

        public List<Coord> FreeCoords = new List<Coord>();

        public BoardRecord(int index)
        {
            Index = index;

            Cells = BoardRecord.IntToInts(index);
            CalculateWinner();
            CalculateFreeCoords();
        }

        private static int[] pow4lookup = { 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144 };

        public static int BoardsToInt(BoardRecord[,] Boards)
        {
            //var index = 0;
            //var result = 0;
            //var value = 0;
            //for (int row = 0; row < 3; row++)
            //{
            //    for (int col = 0; col < 3; col++)
            //    {
            //        index = row * 3 + col;
            //        value = Boards[col, row].Winner;
            //        //result += value * (int)Math.Pow(4, index);
            //        result += value * pow4lookup[index];
            //    }
            //}

            int result = 0;
            result += Boards[0, 0].Winner;
            result += Boards[1, 0].Winner << 2;
            result += Boards[2, 0].Winner << 4;

            result += Boards[0, 1].Winner << 6;
            result += Boards[1, 1].Winner << 8;
            result += Boards[2, 1].Winner << 10;

            result += Boards[0, 2].Winner << 12;
            result += Boards[1, 2].Winner << 14;
            result += Boards[2, 2].Winner << 16;

            return result;
        }

        public static int CellsToInt(Cell[,] Cells)
        {
            int index = 0;
            int result = 0;
            int value = 0;
            for (int row = 0; row < 3; row++)
            {

                for (int col = 0; col < 3; col++)
                {
                    index = row * 3 + col;

                    value = Cells[col, row].Owner;
                    //result += value * (int)Math.Pow(4, index);
                    result += value * pow4lookup[index];
                }
            }
            return result;

            //int result = 0;
            //int value = 0;
            //value = Cells[0, 0].Owner; result += value;
            //value = Cells[1, 0].Owner * 4; result += value;
            //value = Cells[2, 0].Owner * 16; result += value;

            //value = Cells[0, 1].Owner * 64; result += value;
            //value = Cells[1, 1].Owner * 256; result += value;
            //value = Cells[2, 1].Owner * 1024; result += value;

            //value = Cells[0, 2].Owner * 4096; result += value;
            //value = Cells[1, 2].Owner * 16384; result += value;
            //value = Cells[2, 2].Owner * 65536; result += value;
            //return result; ;

        }

        public static int IntsToInt(int[,] Cells)
        {
            var index = 0;
            var result = 0;
            var value = 0;
            for (int row = 0; row < 3; row++)
            {

                for (int col = 0; col < 3; col++)
                {
                    index = row * 3 + col;

                    value = Cells[col, row];
                    //result += value * (int)Math.Pow(4, index);
                    result += value * pow4lookup[index];
                }
            }
            return result;
        }

        public static Cell[,] IntToCells(int index)
        {
            var tmpindex = 0;
            var cellValue = 0;
            var result = new Cell[3, 3];
            for (int row = 0; row < 3; row++)
            {

                for (int col = 0; col < 3; col++)
                {
                    tmpindex = row * 3 + col;

                    cellValue = index % 4;

                    result[col, row] = new Cell(col, row) { Owner = cellValue };
                    //result[col, row].Owner = cellValue;
                    //index -= cellValue;
                    //index /= 4;
                    index = index >> 2;
                }
            }
            return result;
        }

        public static int[,] IntToInts(int index)
        {
            var tmpindex = 0;
            var cellValue = 0;
            var result = new int[3, 3];
            for (int row = 0; row < 3; row++)
            {

                for (int col = 0; col < 3; col++)
                {
                    tmpindex = row * 3 + col;

                    cellValue = index % 4;

                    result[col, row] = cellValue;
                    //index -= cellValue;
                    //index /= 4;
                    index = index >> 2;
                }
            }
            return result;
        }

        private void CalculateWinner()
        {
            //vertical 3x times
            for (int x = 0; x < 3; x++)
            {
                if ((Cells[x, 0] == Cells[x, 1]) && (Cells[x, 1] == Cells[x, 2]) && (Cells[x, 2] != 3))
                {
                    Winner = (Winner | Cells[x, 0]);
                }
            }
            //horizontal 3 times
            for (int x = 0; x < 3; x++)
            {
                if ((Cells[0, x] == Cells[1, x]) && (Cells[1, x] == Cells[2, x]) && (Cells[2, x] != 3))
                {
                    Winner = (Winner | Cells[0, x]);
                }
            }
            //diag leftupper rightbottom
            if ((Cells[0, 0] == Cells[1, 1]) && (Cells[1, 1] == Cells[2, 2]) && (Cells[2, 2] != 3))
            {
                Winner = (Winner | Cells[0, 0]);
            }
            //diag rightupper leftbottom
            if ((Cells[2, 0] == Cells[1, 1]) && (Cells[1, 1] == Cells[0, 2]) && (Cells[0, 2] != 3))
            {
                Winner = (Winner | Cells[2, 0]);
            }
        }

        private void CalculateFreeCoords()
        {
            for (int row = 0; row < 3; row++)
            {
                for (int col = 0; col < 3; col++)
                {
                    if (Cells[col, row] == 0)
                    {
                        FreeCoords.Add(new Coord() { Column = col, Row = row });
                    }
                }
            }

            //bord vol ? dan is er geen winnaar als er geen winnaar is !
            if (FreeCoords.Count == 0)
            {
                if (Winner == 0)
                {
                    Winner = 3;
                }

            }
        }




    }


}
