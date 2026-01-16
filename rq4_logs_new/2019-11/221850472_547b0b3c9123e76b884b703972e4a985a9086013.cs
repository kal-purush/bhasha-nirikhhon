using System;
using System.Data.SqlClient;

namespace DAO.Zoo
{
    public abstract class Conexao : IDisposable
    {
        const string stringConexaoCurso = "Server=localhost\\SQLEXPRESS;Database=futxiacaiada;Trusted_Connection=True;";
        private SqlConnection Conn { get; set; }
        protected SqlCommand ExecutorSql { get; set; }

        public Conexao()
        {
            Conn = new SqlConnection(stringConexaoCurso);
            Conn.Open();
            ExecutorSql = Conn.CreateCommand();
        }

        public void Dispose()
        {
            Conn.Close();
        }
    }
}