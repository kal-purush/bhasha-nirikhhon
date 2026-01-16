using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ToDoList.Model
{
    public class DatabaseCommandSelectTable : DatabaseCommandFetchTable
    {
        public DatabaseCommandSelectTable()
        {
        }

        public override Response<TableReference> ExecuteCommand(string ConnectionString, string Query, int ColNum)
        {
            TableReference tablereference = new TableReference();
            Response<TableReference> response = new Response<TableReference>();
            response.Data = tablereference;

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
                        SelectReference reference = new SelectReference();
                        reference.CustomReport(ColNum);
                        for (int i = 0; i < ColNum; i++)
                        {
                            reference.Value.Add(reader.GetString(reference.ColumnReference[i]));
                        }
                        response.Data.ListOfValues.Add(reference);
                    }
                }
                databaseConnection.Close();
                response.Success = true;
            }
            catch (Exception)
            {
                response.FailedToContactServer();
            }
            return response;
        }
    }
}