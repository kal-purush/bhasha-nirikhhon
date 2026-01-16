using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Lernsoftware
{
    class MySQLDao
    {
        private static MySqlConnection connection;
        static string connectionString = String.Format(
            "user id={0}; password={1}; database={2}", "localhost", "", "lernsoftwaredb");

        public static string ConnectionString
        {
            get => connectionString;
            set => connectionString = value;
        }

        public static void openConnection()
        {
            connection = new MySqlConnection(connectionString);
            connection.Open();
        }

        public static void closeConnection()
        {
            connection.Close();
        }
    }
}