namespace Mishbetzet
{
    /// <summary>
    /// Renders the game.
    /// </summary>
    internal class GameRenderer
    {
        string _tileLeftEdge = "[";
        string _tileRightEdge = "]";

        public GameRenderer(Tilemap tilemap)
        {
            for (int i = 0; i < tilemap.Height; i++)
            {
                for (int j = 0; j < tilemap.Width; j++)
                {
                    Console.Write($"{_tileLeftEdge} {_tileRightEdge}");
                }
                ChangeTileStyle((TileStyle)Random.Shared.Next(0,4));
                Console.ForegroundColor = (ConsoleColor)Random.Shared.Next(0, 16);
                Console.WriteLine();
            }
            Console.ForegroundColor = ConsoleColor.White;
        }

        enum TileStyle
        {
            Square,
            Curly,
            Parenthesis,
            LessGreater
        }

        void ChangeTileStyle(TileStyle choosenTile)
        {
            switch (choosenTile)
            {
                case TileStyle.Square:
                    _tileLeftEdge = "[";
                    _tileRightEdge = "]";
                    break;
                case TileStyle.Curly:
                    _tileLeftEdge = "{";
                    _tileRightEdge = "}";
                    break;
                case TileStyle.Parenthesis:
                    _tileLeftEdge = "(";
                    _tileRightEdge = ")";
                    break;
                case TileStyle.LessGreater:
                    _tileLeftEdge = "<";
                    _tileRightEdge = ">";
                    break;
            }
        }
    }
}