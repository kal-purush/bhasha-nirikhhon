using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;

namespace Dados
{
    public class DFuncionario
    {
        private int _id;
        private String _nome;
        private String _acesso;
        private String _usuario;
        private String _senha;

        public int Id
        {
            get { return _id; }
            set { _id = value; }
        }

        public String Nome
        {
            get { return _nome; }
            set { _nome = value; }
        }

        public String Acesso
        {
            get { return _acesso; }
            set { _acesso = value; }
        }

        public String Usuario
        {
            get { return _usuario; }
            set { _usuario = value; }
        }

        public String Senha
        {
            get { return _senha; }
            set { _senha = value; }
        }

        //Construtor Vazio
        public DFuncionario()
        {

        }

        //Construtor com Parametros
        public DFuncionario(int id, String nome, String acesso, String usuario, String senha)
        {
            this.Id = id;
            this.Nome = nome;
            this.Acesso = acesso;
            this.Usuario = usuario;
            this.Senha = senha;
        }

        //Metodo listar
        public DataTable Listar()
        {
            DataTable DTResultado = new DataTable("funcionario");
            SqlConnection SqlCon = new SqlConnection();

            try
            {
                SqlCon.ConnectionString = Conexao.Cn;
                SqlCommand SqlCmd = new SqlCommand();
                SqlCmd.Connection = SqlCon;
                SqlCmd.CommandText = "splistar_funcionario";
                SqlCmd.CommandType = CommandType.StoredProcedure;

                SqlDataAdapter SqlDA = new SqlDataAdapter(SqlCmd);
                SqlDA.Fill(DTResultado);
            }
            catch (Exception e)
            {
                DTResultado = null;
            }
            return DTResultado;
        }
    }
}