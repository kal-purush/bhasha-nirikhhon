using Cocona;

namespace ChessApiConsoleClient.Commands
{
    public static class CoconaCommandMapper
    {
        private static void NewGame()
        {
            
        }

        private static void ListGames()
        {
        }

        private static void ShowGameById(string game)
        {
        }

        private static void ShowAllowedMoves(string game, string cell)
        {
        }

        private static void MovePiece(string game, string from, string to)
        {
            
        }

        public static void MapEndPoints(this CoconaApp app)
        {
            app.AddCommand("new", NewGame);
            app.AddCommand("list", ListGames);
            app.AddCommand("show", NewGame);
            app.AddCommand("moves", NewGame);
            app.AddCommand("move", NewGame);
        }
    }
}