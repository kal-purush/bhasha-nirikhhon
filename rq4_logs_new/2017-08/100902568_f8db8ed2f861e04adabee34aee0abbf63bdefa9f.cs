using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace MySQLHelper
{
    public static class Connections
    {

        private static MySqlConnection Connect(string database, string username, string password)
        {
            MySqlConnection conn = null;

            string connstring = $"Server=localhost; database={database}; UID={username}; password={password}";



            return conn;
        }

        public enum MySQLDatabases
        {
            
        }

    }
}