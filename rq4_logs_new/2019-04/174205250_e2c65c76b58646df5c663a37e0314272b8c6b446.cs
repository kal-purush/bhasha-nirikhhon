using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Train_UI
{
    class DBUtils
    {
        public static MySqlConnection GetDBConnection()
        {
            string host = "localhost";
            int port = 3306;
            string database = "cis3755";
            string username = "root";
            string password = "wwww";

            return DBMySQLUtils.GetDBConnection(host, port, database, username, password);
        }
    }
}