using System.Data;
using System.Data.SqlClient;

namespace APP
{
    public class DbConnection
    {
        private SqlConnection _connection;
        private SqlCommand _command;
        public static string ConnectionString { get; set; }

        public void OpenConnection()
        {
            _connection = new SqlConnection(ConnectionString);
            _connection.Open();
        }

        public void CloseConnection()
        {
            _connection.Close();
        }

        public DataTable ExcuteReader(string sql)
        {
            var table = new DataTable();
            var adapter = new SqlDataAdapter(sql, _connection);
            adapter.Fill(table);
            return table;
        }

        public void ExcuteNonQuery(string sql)
        {
            _command = new SqlCommand(sql, _connection);
            _command.ExecuteNonQuery();
        }

        public object ExecuteScalar(string sql)
        {
            _command = new SqlCommand(sql, _connection);
            return _command.ExecuteScalar();
        }
    }
}