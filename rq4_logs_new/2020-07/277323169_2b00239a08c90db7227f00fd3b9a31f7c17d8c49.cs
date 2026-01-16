using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using System.Data.SqlClient;
using PIGUE.Models;
using System.Data.Common;

namespace PIGUE.DataAccess
{
    public class PlayerRepository
    {
        const string ConnectionString = "Server=localhost;Database=Pigue;Trusted_Connection=True;";
        //GET ALL PLAYERS
        public IEnumerable<Player> GetAllPlayers()
        {
            using (var db = new SqlConnection(ConnectionString))
            {
                return db.Query<Player>("select * from players");
            }
        }
    }
}