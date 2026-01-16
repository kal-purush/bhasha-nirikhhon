using Attack.Game;
using Microsoft.Data.Sqlite;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Attack.Saves.SQLLite
{
    internal class SaveStartingLocations : BaseSave
    {
        public SaveStartingLocations()
        {
        }

        public bool HasStartLocations(GameInstance game)
        {

            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();

                command.CommandText = @"SELECT EXISTS (SELECT Id FROM Positions WHERE GameId = $id) as Result;";

                command.Parameters.AddWithValue("$id", game.Id);

                try
                {
                    var result = command.ExecuteScalar();

                    Log.Debug($"Successfully queried for game");

                    return Convert.ToBoolean(result);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"Failed to save location {game.Id}");
                    throw;
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        public void Save(GameInstance game)
        {

            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();

                command.CommandText =
                    @"
                        INSERT INTO Positions (PieceId, GameId, StartX, StartY, Player)
                        VALUES($pieceId, $gameId, $startX, $startY, $player);
                    ";

                foreach (var tile in game.StartingPositions)
                {
                    command.Parameters.Clear();

                    var piece = tile.Piece;
                    var position = tile.Position;

                    command.Parameters.AddWithValue("$pieceId", (int)piece.PieceType);
                    command.Parameters.AddWithValue("$gameId", game.Id);
                    command.Parameters.AddWithValue("$startX", position.X);
                    command.Parameters.AddWithValue("$startY", position.Y);
                    command.Parameters.AddWithValue("$player", (int)piece.Team);

                    try
                    {
                        command.ExecuteNonQuery();

                        Log.Debug($"Successfully saved location {piece.PieceType}:{piece.Team} - {game.Id}");
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, $"Failed to save location {game.Id}");
                        throw;
                    }
                }

            }

        }
    }
}