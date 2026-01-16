using Microsoft.Data.Sqlite;
using System;

namespace Excel
{
    public class DBLogic
    {
        SqliteConnection sqlite_conn;
        public void CreateConnection( string path )
        {

            SqliteConnection conn;
            // Create a new database connection:
            conn = new SqliteConnection($@"Data Source={path}");
         // Open the connection:
            try
            {
                conn.Open();
                sqlite_conn = conn;
                Table();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            
        }

        public void Table()
        {

            SqliteCommand sqlite_cmd;
            string Createsql = "CREATE TABLE IF NOT EXISTS SampleTable(Col1 VARCHAR(20), Col2 INT)";
            string Createsql1 = "CREATE TABLE IF NOT EXISTS SampleTable1(Col1 VARCHAR(20), Col2 INT)";
            sqlite_cmd = sqlite_conn.CreateCommand();
            sqlite_cmd.CommandText = Createsql;
            sqlite_cmd.ExecuteNonQuery();
            sqlite_cmd.CommandText = Createsql1;
            sqlite_cmd.ExecuteNonQuery();
        }
    }
}