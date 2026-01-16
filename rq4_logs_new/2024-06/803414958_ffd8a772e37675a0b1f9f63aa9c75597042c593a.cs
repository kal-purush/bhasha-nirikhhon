using Attack.Game;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Attack.Util;
using Serilog;

namespace Attack.Saves.SQLLite
{
    internal class SaveTurn : BaseSave
    {

        public SaveTurn() { }

        public void Save(GameInstance game, Turn turn)
        {
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                var parameters = command.Parameters;

                command.CommandText =
                    @"
                        INSERT INTO Turns (Game, StartX, StartY, EndX, EndY, AttackX, AttackY, DateTime)
                        VALUES($gameId, $startX, $startY, $endX, $endY, $attackX, $attackY, $dateTime)
                    ";

                parameters.AddWithValue("$gameId", game.Id);

                var startPosition = turn.SelectedTile.Position;

                parameters.AddWithValue("$startX", startPosition.X);
                parameters.AddWithValue("$startY", startPosition.Y);

                if (turn.DestinationTile == null) 
                {
                    parameters.AddWithValue("$endX", DBNull.Value);
                    parameters.AddWithValue("$endY", DBNull.Value);
                }
                else
                {
                    var endPosition = turn.DestinationTile.Position;

                    parameters.AddWithValue("$endX", endPosition.X);
                    parameters.AddWithValue("$endY", endPosition.Y);
                }

                if (turn.AttackedTile == null)
                {
                    parameters.AddWithValue("$attackX", DBNull.Value);
                    parameters.AddWithValue("$attackY", DBNull.Value);
                }
                else
                {
                    var attackPosition = turn.AttackedTile.Position;

                    parameters.AddWithValue("$attackX", attackPosition.X);
                    parameters.AddWithValue("$attackY", attackPosition.Y);
                }

                var now = DateTime.UtcNow;

                parameters.AddWithValue("$dateTime", now.ToString(Constants.DateStringFormat));

                try
                {
                    command.ExecuteNonQuery();

                    Log.Debug($"Successfully saved turn for {game.Id}");
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"Failed to save turn for {game.Id}");
                    throw;
                }
            }
        }
    }
}