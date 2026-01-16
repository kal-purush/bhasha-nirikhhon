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
    public class BBLSubCategoria
    {
        private DalConexao conexao;

        public BBLSubCategoria(DalConexao cx)
        {
            this.conexao = cx;
        }
        public void Incluir(ModeloSubcategoria Modelo)
        {
            if ((Modelo.ScatNome.Trim().Length == 0) && (Modelo.CatCod <= 0))
            {
                throw new Exception("Nome Descrição e Categoria  não pode estar vazio");
            }
            //Modelo.CatNome = Modelo.CatNome.ToUpper();//deixa letra toda maiuscula
            DalSubCategoria Dalobj = new DalSubCategoria(conexao);
            Dalobj.Incluir(Modelo);
        }
        public void Alterar(ModeloSubcategoria Modelo)
        {
            if ((Modelo.ScatCod <= 0) && (Modelo.ScatNome.Trim().Length == 0) && (Modelo.CatCod <= 0))
            {
                throw new Exception("Necessario dados completos para alterar");
            }

            DalSubCategoria Dalobj = new DalSubCategoria(conexao);
            Dalobj.Alterar(Modelo);

        }
        public void Excluir(int codigo)
        {
            DalSubCategoria Dalobj = new DalSubCategoria(conexao);
            Dalobj.Excluir(codigo);
        }
        public DataTable localizar(String valor)
        {
            DalSubCategoria Dalobj = new DalSubCategoria(conexao);
            return Dalobj.localizar(valor);
        }
        public ModeloSubcategoria CarragaModelosubCategoria(int codigo)
        {
            DalSubCategoria Dalobj = new DalSubCategoria(conexao);
            return Dalobj.CarragaModeloSubCategoria(codigo);
        }
        public DataTable LocalizarPorCategoria(int categoria)
        {
            DalSubCategoria DALobj = new DalSubCategoria(conexao);
            return DALobj.LocalizarPorCategoria(categoria);
        }
    }
}