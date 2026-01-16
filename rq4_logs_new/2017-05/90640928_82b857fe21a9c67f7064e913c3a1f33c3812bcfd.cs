using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using AcessoBancoDados.Properties;
namespace AcessoBancoDados
{
    // modelo de classe apresentada com o professor Drausio 
    // comparar o que será aprendido aqui com os conhecimetos adquirido
    // até aqui com os conceitos apresentado pelo kerplunk
    public class AcessoDadosSql
    {
        // criar conexão
        private SqlConnection CriarConexao()
        {
            return new SqlConnection(Settings.Default.stringDeConexao);
        }
        //Parâmetros que vão para o banco
        private SqlParameterCollection sqlParameterCollection = new SqlCommand().Parameters;
        public void LimparParametros()
        {
            sqlParameterCollection.Clear();
        }
        public void AdicionaParamtros(string nomeParametro, object valorParametro)
        {
            sqlParameterCollection.Add(new SqlParameter(nomeParametro, valorParametro));
        }
        // pesistência com inserir/alterar/deletar
        public object ExecutarManipulação(CommandType commandType, string nomeStoreProcedure)
        {
            try
            {
                //Criar conexão, o mysqlConection declarado abaixo tem o retorno do método
                //Criar conexão declarado logo acima 
                SqlConnection sqlConection = CriarConexao();
                //Abrir conexão
                sqlConection.Open();
                // Criar comando que vai levar as infromações para o banco 
                SqlCommand sqlcommand = sqlConection.CreateCommand();
                // colocando as coisa que vão trafegar dentro da caixa de conexão
                sqlcommand.CommandType = commandType;
                sqlcommand.CommandText = nomeStoreProcedure;
                //Comando Time out é responsável por deterimnar o tempo de espera
                // para realizar uma deterimada ação com tempo em segundos
                sqlcommand.CommandTimeout = 200;

                // Adicionar os parãmetros
                // para cada mysqlparameters na mysqlparametercollection 
                foreach (SqlParameter sqlparameter in sqlParameterCollection)
                {
                    // entrei peguei o primeiro parametro e coloquei na "caixa" 
                    //declarado á cima e estou adicionadno novos comandos 
                    // com nome e valor
                    sqlcommand.Parameters.Add(new SqlParameter(sqlparameter.ParameterName, sqlparameter.Value));
                }
                //executa a query ou seja manda a execução para o banco
                return sqlcommand.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        //Consula os registros do BD/ Pesquisar
        public DataTable ExecutarConsulta(CommandType commandType, string nomeStoreProcedure)
        {
            try
            {
                //Criar conexão, o mysqlConection declarado abaixo tem o retorno do método
                //Criar conexão declarado logo acima 
                SqlConnection sqlConection = CriarConexao();
                //Abrir conexão
                sqlConection.Open();
                // Criar comando que vai levar as infromações para o banco 
                SqlCommand sqlcommand = sqlConection.CreateCommand();
                // colocando as coisa que vão trafegar dentro da caixa de conexão
                sqlcommand.CommandType = commandType;
                sqlcommand.CommandText = nomeStoreProcedure;
                //Comando Time out é responsável por deterimnar o tempo de espera
                // para realizar uma deterimada ação com tempo em segundos
                sqlcommand.CommandTimeout = 200;

                // Adicionar os parãmetros
                // para cada mysqlparameters na mysqlparametercollection 
                foreach (SqlParameter sqlparameter in sqlParameterCollection)
                {
                    // entrei peguei o primeiro parametro e coloquei na "caixa" 
                    //declarado á cima e estou adicionadno novos comandos 
                    // com nome e valor
                    sqlcommand.Parameters.Add(new SqlParameter(sqlparameter.ParameterName, sqlparameter.Value));
                }
                SqlDataAdapter sqldataAdaper = new SqlDataAdapter(sqlcommand);
                // criar um datatable vazio
                DataTable datatable = new DataTable();
                sqldataAdaper.Fill(datatable);
                return datatable;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
    }
}