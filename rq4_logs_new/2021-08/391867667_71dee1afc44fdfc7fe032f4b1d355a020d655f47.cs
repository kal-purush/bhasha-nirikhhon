using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ToDoList.Model
{
    public class DatabaseCommandCount : DatabaseCommand
    {
        public DatabaseCommandCount()
        {
        }

        public override Response<string> ExecuteCommand(string ConnectionString, string Query)
        {
            Response<string> response = new Response<string>();
            MySqlConnection databaseConnection = new MySqlConnection(ConnectionString);
            MySqlCommand commandDatabase = new MySqlCommand(Query, databaseConnection);
            commandDatabase.CommandTimeout = 60;
            MySqlDataReader reader;

            try
            {
                databaseConnection.Open();
                reader = commandDatabase.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        response.Data = reader.GetString(0);
                    }
                }
                databaseConnection.Close();
            }
            catch (Exception)
            {
                response.FailedToContactServer();
            }
            return response;
        }
    }
}