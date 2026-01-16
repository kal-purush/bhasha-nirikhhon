using System.Windows.Documents;
using TicTacToe.Classes;
using TicTacToe.Enums;

namespace TicTacToe.Helpers
{
    public class BoardLine
    {
        public int Number { get; set; }
        public SymbolTypeEnum? Symbol { get; set; }
    }
    public static class ComputerPlayerHelper
    {
        public static int? GetMove(List<GameCell> gameCells, List<List<int>> winLines, SymbolTypeEnum symbol)
        {
            var oppositeSymbol = symbol == SymbolTypeEnum.X ? SymbolTypeEnum.O : SymbolTypeEnum.X;

            var board = GetBoard(gameCells, winLines);

            //var canWinLines
            //Can Win
            foreach (var boardLines in board)
            {
                if (boardLines.Where(boardLine => boardLine.Symbol.HasValue && boardLine.Symbol.Value == symbol).Count() == 2)
                {
                    var boardCell = boardLines.Find(boardLine => !boardLine.Symbol.HasValue);
                    if (boardCell != null) return boardCell.Number;
                }
            }

            //Prevent Opponent Win
            foreach (var boardLines in board)
            {
                if (boardLines.Where(boardLine => boardLine.Symbol.HasValue && boardLine.Symbol.Value == oppositeSymbol).Count() == 2)
                {
                    var boardCell = boardLines.Find(boardLine => !boardLine.Symbol.HasValue);
                    if (boardCell != null) return boardCell.Number;
                }
            }


            var freeCells = gameCells.Where(cell => !cell.Symbol.HasValue).ToList();
            if (freeCells.Count == 0) return null;
            var random = new Random();
            var randomCell = freeCells[random.Next(freeCells.Count)];

            return randomCell.Number;
        }

        private static List<List<BoardLine>> GetBoard(List<GameCell> cells, List<List<int>> winLines)
        {
            var board = new List<List<BoardLine>>();
            foreach (var winLine in winLines)
            {
                var lines = cells
                    .Where(cell => winLine.Contains(cell.Number))
                    .Select(cell => new BoardLine { Number = cell.Number, Symbol = cell.Symbol })
                    .ToList();
                board.Add(lines);
            }

            return board;
        }
    }
}