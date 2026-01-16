using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace HOSPITAL_MANAGEMENT_SOURCE.DAL
{
    public static class SqlResult
    {
        //Get ConnectionString in app.config
        private static string sqlConnectString = ConfigurationManager.ConnectionStrings[""].ConnectionString;

        //Execute insert, update, delete command without parameters
        public static int ExecuteNonQuery(string commandString)
        {
            using (SqlConnection connection = new SqlConnection(sqlConnectString))
            {
                SqlCommand command = new SqlCommand(commandString, connection);
                connection.Open();
                return command.ExecuteNonQuery();
            }
        }

        //Execute insert, update, delete command with parameters
        public static int ExecuteNonQuery(string commandString, SqlParameter[] sqlParametes)
        {
            using (SqlConnection connection = new SqlConnection(sqlConnectString))
            {
                SqlCommand command = new SqlCommand(commandString, connection);
                command.Parameters.AddRange(sqlParametes);
                connection.Open();
                return command.ExecuteNonQuery();
            }
        }

        //Execute select without parameters
        public static DataTable ExecuteQuery(string commandString)
        {
            SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(commandString, sqlConnectString);
            DataTable dataTable = new DataTable();
            sqlDataAdapter.Fill(dataTable);
            return dataTable;
        }

        //Execute select with parameters
        public static DataTable ExecuteQuery(string commandString, SqlParameter[] sqlParametes)
        {
            SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(commandString, sqlConnectString);
            sqlDataAdapter.SelectCommand.Parameters.AddRange(sqlParametes);
            DataTable dataTable = new DataTable();
            sqlDataAdapter.Fill(dataTable);
            return dataTable;
        }

        //Execute scalar command without parameters
        public static object ExecuteScalar(string commandString)
        {
            using (SqlConnection connection = new SqlConnection(sqlConnectString))
            {
                SqlCommand command = new SqlCommand(commandString, connection);
                connection.Open();
                return command.ExecuteScalar();
            }
        }

        //Execute scalar command with parameters
        public static object ExecuteScalar(string commandString, SqlParameter[] sqlParametes)
        {
            using (SqlConnection connection = new SqlConnection(sqlConnectString))
            {
                SqlCommand command = new SqlCommand(commandString, connection);
                command.Parameters.AddRange(sqlParametes);
                connection.Open();
                return command.ExecuteScalar();
            }
        }
    }
}