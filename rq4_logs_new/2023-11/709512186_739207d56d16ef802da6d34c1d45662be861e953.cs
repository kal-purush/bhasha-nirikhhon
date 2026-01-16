using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using TCC_euquero.Modelo;

namespace TCC_euquero.Logica
{
    public class Banco
    {
        string linhaConexao = "";
        MySqlConnection conexao = null;
        MySqlCommand cSQL = null;

        public Banco()
        {
            linhaConexao = "UID=root;PASSWORD=root;DATABASE=euquero;SERVER=localhost;";
        }

        protected void Conectar()
        {
            try
            {
                conexao = new MySqlConnection(linhaConexao);
                conexao.Open();
            }
            catch (Exception)
            {
                throw new Exception("Não foi possível conectar no Servidor");
            }
        }

        protected void Desconectar()
        {
            try
            {
                if (conexao.State == System.Data.ConnectionState.Open)
                    conexao.Close();
            }
            catch (Exception)
            {
                throw new Exception("Não foi possível desconectar do Servidor");
            }
        }

        protected void ExecutarComando(string comando)
        {
            Conectar();
            try
            {
                cSQL = new MySqlCommand(comando, conexao);
                cSQL.ExecuteNonQuery();
            }
            catch (Exception)
            {
                throw new Exception("Não foi possível executar o comando");
            }
            finally
            {
                Desconectar();
            }
        }

        protected void ExecutarProcedure(string nomeSP, List<Parametro> valores)
        {
            Conectar();
            try
            {
                cSQL = new MySqlCommand(nomeSP, conexao);
                cSQL.CommandType = System.Data.CommandType.StoredProcedure;
                cSQL.Parameters.Clear();
                
                if (valores != null)
                    if (valores.Count > 0)
                    {
                        for (int i = 0; i < valores.Count; i++)
                        {
                            cSQL.Parameters.AddWithValue(valores[i].Nome, valores[i].Valor.ToString());
                        }
                    }

                cSQL.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                Desconectar();
            }
        }

        protected MySqlDataReader ConsultarComando(string comando)
        {
            Conectar();
            try
            {
                cSQL = new MySqlCommand(comando, conexao);
                return cSQL.ExecuteReader();
            }
            catch (Exception)
            {
                throw new Exception("Não foi possível ler a consulta do comando");
            }
        }

        protected MySqlDataReader ConsultarProcedure(string nomeSP, List<Parametro> valores)
        {
            Conectar();
            try
            {
                cSQL = new MySqlCommand(nomeSP, conexao);
                cSQL.CommandType = System.Data.CommandType.StoredProcedure;
                cSQL.Parameters.Clear();
                if (valores != null)
                    if (valores.Count > 0)
                    {
                        for (int i = 0; i < valores.Count; i++)
                        {
                            cSQL.Parameters.AddWithValue(valores[i].Nome, valores[i].Valor.ToString());
                        }
                    }

                return cSQL.ExecuteReader();
            }
            catch (Exception)
            {
                throw new Exception("Não foi possível ler a consulta da procedure");
            }
        }
    }
}