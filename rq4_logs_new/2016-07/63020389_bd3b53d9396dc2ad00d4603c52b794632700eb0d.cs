using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;

namespace GvGStats
{
    class DatabaseHandler
    {
        // Member variables
        public SQLiteConnection dataConnection;

        // Methods

        public void OpenConnection()
        {
            dataConnection = new SQLiteConnection(@"Data Source=gvgdata.db;Version=3;"); // might need tweaking
        }

        public void CloseConnection()
        {
            dataConnection.Close();
        }
        
        /// <summary>
        /// Takes in and sends a SQL query then returns the Data Reader
        /// </summary>
        /// <param name="query">Query to the database</param>
        /// <returns></returns>
        public SQLiteDataReader SendQueryAndReturnData(string query)
        {
            SQLiteCommand createCommand = new SQLiteCommand(query, dataConnection);
            SQLiteDataReader dataReader = createCommand.ExecuteReader();
            return dataReader;
        }

    }
}