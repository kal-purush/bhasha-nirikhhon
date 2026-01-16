using System.Data;
using Microsoft.Data.SqlClient;
namespace Libre_ERP_API.Data
{
    public class SqlConnection
    {
        private static readonly string _connectionString = Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING") 
            ?? throw new InvalidOperationException("SQL_CONNECTION_STRING not set.");

        public static async Task<(int ErrorID, string ErrorDescription, int IDReturn)> ExecuteNonQuery(string storedProcedure, SqlParameter[] parameters)
        {
            using var connection = new Microsoft.Data.SqlClient.SqlConnection(_connectionString);
            using var command = new SqlCommand(storedProcedure, connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            AddErrorParameters(command);

            var idReturnParam = new SqlParameter("@ID_RETURN", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            command.Parameters.Add(idReturnParam);

            command.Parameters.AddRange(parameters);

            await connection.OpenAsync();
            await command.ExecuteNonQueryAsync();

            var (errorID, errorDescription) = ExtractErrorInfo(command);
            var idReturn = idReturnParam.Value != DBNull.Value ? (int)idReturnParam.Value : -1;

            return (errorID, errorDescription, idReturn);
        }
        public static async Task<(int ErrorID, string ErrorDescription, int IDReturn)> ExecuteNonQueryWithReturnAsync(string storedProcedure, SqlParameter[] parameters)
        {
            using var connection = new Microsoft.Data.SqlClient.SqlConnection(_connectionString);
            using var command = new SqlCommand(storedProcedure, connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            AddErrorParameters(command);

            var idReturnParam = new SqlParameter("@ID_RETURN", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            command.Parameters.Add(idReturnParam);

            command.Parameters.AddRange(parameters);

            await connection.OpenAsync();
            await command.ExecuteNonQueryAsync();

            var (errorID, errorDescription) = ExtractErrorInfo(command);
            var idReturn = idReturnParam.Value != DBNull.Value ? (int)idReturnParam.Value : -1;

            return (errorID, errorDescription, idReturn);
        }

        public static async Task<(List<T> Data, int ErrorID, string ErrorDescription)> ExecuteReaderAsync<T>(string storedProcedure, SqlParameter[] parameters, Func<SqlDataReader, T> mapFunction)
        {
            var data = new List<T>();

            using var connection = new Microsoft.Data.SqlClient.SqlConnection(_connectionString);
            using var command = new SqlCommand(storedProcedure, connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            AddErrorParameters(command);
            command.Parameters.AddRange(parameters);

            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                data.Add(mapFunction(reader));
            }

            reader.Close();

            var (errorID, errorDescription) = ExtractErrorInfo(command);
            return (data, errorID, errorDescription);
        }


        private static void AddErrorParameters(SqlCommand command)
        {
            command.Parameters.Add(new SqlParameter("@ERROR_ID", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            });

            command.Parameters.Add(new SqlParameter("@ERROR_DESCRIPTION", SqlDbType.NVarChar, -1)
            {
                Direction = ParameterDirection.Output
            });
        }
        private static (int ErrorID, string ErrorDescription) ExtractErrorInfo(SqlCommand command)
        {
            int errorID = command.Parameters["@ERROR_ID"].Value is int id ? id : -1;
            string errorDescription = command.Parameters["@ERROR_DESCRIPTION"].Value?.ToString() ?? "Unknown error.";

            return (errorID, errorDescription);
        }
    }
}