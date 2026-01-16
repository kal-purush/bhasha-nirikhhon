using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace PopLibrary
{
    public class SqlAdapter
    {
        private SqlConnection _sqlConnection;
        public SqlAdapter(SqlSettings sqlSettings)
        {
            _sqlConnection = new SqlConnection(sqlSettings.PopDbConnectionString);
        }

        public List<T> ExecuteStoredProcedureAsync<T>()
        {
            var ret = new List<T>();

            _sqlConnection.Open();

            SqlCommand cmd = new SqlCommand("dbo.GetAuctions", _sqlConnection);
            cmd.CommandType = CommandType.StoredProcedure;

            using (SqlDataReader rdr = cmd.ExecuteReader())
            {
                ret = JsonConvert.DeserializeObject<List<T>>(SqlDatoToJson(rdr));
            }
            _sqlConnection.Close();
            return ret;
        }

        private string SqlDatoToJson(SqlDataReader dataReader)
        {
            var dataTable = new DataTable();
            dataTable.Load(dataReader);
            string JSONString = string.Empty;
            JSONString = JsonConvert.SerializeObject(dataTable);
            return JSONString;
        }
    }
}