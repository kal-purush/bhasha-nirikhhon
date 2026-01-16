using AcessoBancoDados.Properties;
using System;
using System.Data;
using System.Data.SqlClient;

namespace AcessoBancoDados
{
    public class AcessoDadosSQLServer
    {
        //Cria a conexao
        private SqlConnection criarConexao()
        {
            return new SqlConnection(Settings.Default.stringConexao);
        }

        private SqlParameterCollection sqlPC = new SqlCommand().Parameters;

        public void limparParametros()
        {
            sqlPC.Clear();
        }

        public void addParametros(string nomeParametro, object valorParametro)
        {
            sqlPC.Add(new SqlParameter(nomeParametro, valorParametro));
        }

        //Inserir - Alterar - Excluir
        public object executarAlteracao(CommandType ct, string nomeStoredProcedure)
        {
            try
            {
                SqlConnection sqlConnection = criarConexao();
                sqlConnection.Open();

                SqlCommand sqlCommand = sqlConnection.CreateCommand();
                sqlCommand.CommandType = ct;
                sqlCommand.CommandText = nomeStoredProcedure;
                sqlCommand.CommandTimeout = 7200;

                foreach (SqlParameter sqlParameter in sqlPC)
                {
                    sqlCommand.Parameters.Add(new SqlParameter(sqlParameter.ParameterName, sqlParameter.Value));
                }
                return sqlCommand.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        //Consultar registro
        public DataTable executarConsulta(CommandType ct, string nomeStoredProcedure)
        {
            try
            {
                SqlConnection sqlConnection = criarConexao();
                sqlConnection.Open();

                SqlCommand sqlCommand = sqlConnection.CreateCommand();
                sqlCommand.CommandType = ct;
                sqlCommand.CommandText = nomeStoredProcedure;
                sqlCommand.CommandTimeout = 7200;
                foreach (SqlParameter sqlParameter in sqlPC)
                {
                    sqlCommand.Parameters.Add(new SqlParameter(sqlParameter.ParameterName, sqlParameter.Value));
                }

                SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand);
                DataTable dataTable = new DataTable();
                sqlDataAdapter.Fill(dataTable);

                return dataTable;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
    }
}