using UnityEngine;
using System.Data;
using Mono.Data.Sqlite;
using System.IO;
using System.Collections.Generic;

public class DatabaseManager : MonoBehaviour
{
    private string connectionString;
    private IDbConnection dbConnection;

    public bool needsRunning;

    // Start is called before the first frame update
    void Start()
    {

        if (!needsRunning) return;

        connectionString = "URI=file:" + Application.dataPath + "/BrainstormDB";
        dbConnection = new SqliteConnection(connectionString);
        dbConnection.Open();

        //Creates the tables in the database
        foreach(var query in Queries.CreateTablesQueries)
        {
            ExecuteQueryWithNoReturn(query);
        }

        //Inserts data into tables
        foreach(var query in Queries.InsertIntoSubjectsQueries)
        {
            ExecuteQueryWithNoReturn(query);
        }

        dbConnection.Close();

        needsRunning = false;
    }

    private void ExecuteQueryWithNoReturn(string query)
    {
        IDbCommand dbCommand = dbConnection.CreateCommand();
        dbCommand.CommandText = query;
        dbCommand.ExecuteNonQuery();
    }

    private object ExecuteQueryWithSingleValueReturn(string query)
    {
        IDbCommand dbCommand = dbConnection.CreateCommand();
        dbCommand.CommandText = query;
        var result = dbCommand.ExecuteScalar();

        return result;
    }

    private List<List<object>> ExecuteQueryWithMultiValueReturn(string query)
    {
        var result = new List<List<object>>();

        IDbCommand dbCommand = dbConnection.CreateCommand();
        dbCommand.CommandText = query;

        var reader = dbCommand.ExecuteReader();

        while (reader.Read())
        {
            var row = new List<object>();

            for (int i = 0; i < reader.FieldCount; i++)
            {
                row.Add(reader.GetValue(i));
            }

            result.Add(row);
        }

        return result;
    }
}