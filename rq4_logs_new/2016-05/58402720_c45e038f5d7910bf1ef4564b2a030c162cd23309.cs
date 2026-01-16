using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DAL;
using Modelo;
using System.Data;
using System.Data.SqlClient;


namespace BBL
{
    public class BBLCategoria
    {
        private DalConexao conexao;

        public BBLCategoria(DalConexao cx)
        {
            this.conexao = cx;
        }
        public void Incluir(ModeloCategoria Modelo)
        {
            if (Modelo.CatNome.Trim().Length == 0)
            {
                throw new Exception("Nome categoria não pode estar vazio");
            }
            //Modelo.CatNome = Modelo.CatNome.ToUpper();//deixa letra toda maiuscula
            DalCategoria Dalobj = new DalCategoria(conexao);
            Dalobj.Incluir(Modelo);
        }
        public void Alterar(ModeloCategoria Modelo)
        {
            if( (Modelo.CatCod <= 0) && (Modelo.CatNome.Trim().Length==0))
            {
                throw new Exception("Codigo e nome da categoria são obrigatorios");
            }
            
            DalCategoria Dalobj = new DalCategoria(conexao);
            Dalobj.Alterar(Modelo);
            
        }
        public void Excluir(int codigo)
        {
            DalCategoria Dalobj = new DalCategoria(conexao);
            Dalobj.Excluir(codigo);
        }
        public DataTable localizar(String valor)
        {
            DalCategoria Dalobj = new DalCategoria(conexao);
            return Dalobj.localizar(valor);
        }
        public ModeloCategoria CarragaModeloCategoria(int codigo)
        {
            DalCategoria Dalobj = new DalCategoria(conexao);
            return Dalobj.CarragaModeloCategoria(codigo);
        }
    }
   

}