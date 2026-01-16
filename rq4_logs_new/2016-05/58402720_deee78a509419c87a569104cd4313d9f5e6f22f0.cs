using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//add
using Modelo;
using DAL;
using Ferramentas;
using System.Data;

namespace BBL
{
    public class BLLFornecedor
    {
        private DalConexao conexao;
        public BLLFornecedor(DalConexao cx)
        {
            this.conexao = cx;
        }

        public void Incluir(ModeloFornecedor modelo)
        {
            if (modelo.ForNome.Trim().Length == 0)
            {
                throw new Exception("O nome do Fornecedor é obrigatório");
            }

                        //VALIDAÇÕES /////
            /*
            if (modelo.ForCnpj.Trim().Length == 0)
            {
                throw new Exception("O CNPJ do Fornecedor é obrigatório");
            }

            if (Validacao.IsCnpj(modelo.ForCnpj) == false)
            {
                throw new Exception("O CNPJ inválido");
            }

            //verificar CPF/CNPJ

            if (modelo.ForIe.Trim().Length == 0)
            {
                throw new Exception("O IE do Fornecedor é obrigatório");
            }

            if (modelo.ForFone.Trim().Length == 0)
            {
                throw new Exception("O telefone do Fornecedor é obrigatório");
            }

            // For_tipo = 0 -> fisica
            // For_tipo = 1 -> juridica

              */
            DalFornecedor DALobj = new DalFornecedor(conexao);
            DALobj.Incluir(modelo);
        }

        public void Alterar(ModeloFornecedor modelo)
        {
            if (modelo.ForNome.Trim().Length == 0)
            {
                throw new Exception("O nome do Fornecedor é obrigatório");
            }

            if (modelo.ForCnpj.Trim().Length == 0)
            {
                throw new Exception("O CNPJ do Fornecedor é obrigatório");
            }

            if (Validacao.IsCnpj(modelo.ForCnpj) == false)
            {
                throw new Exception("O CNPJ inválido");
            }

            if (modelo.ForIe.Trim().Length == 0)
            {
                throw new Exception("O IE do Fornecedor é obrigatório");
            }

            if (modelo.ForFone.Trim().Length == 0)
            {
                throw new Exception("O telefone do Fornecedor é obrigatório");
            }

            DalFornecedor DALobj = new DalFornecedor(conexao);
            DALobj.Alterar(modelo);
        }

        public void Excluir(int codigo)
        {
            DalFornecedor DALobj = new DalFornecedor(conexao);
            DALobj.Excluir(codigo);
        }
        public DataTable Localizar(String valor)
        {
            DalFornecedor DALobj = new DalFornecedor(conexao);
            return DALobj.Localizar(valor);
        }

        public DataTable LocalizarPorNome(String valor)
        {
            DalFornecedor DALobj = new DalFornecedor(conexao);
            return DALobj.LocalizarPorNome(valor);
        }
        public DataTable LocalizarPorCNPJ(String valor)
        {
            DalFornecedor DALobj = new DalFornecedor(conexao);
            return DALobj.LocalizarPorCNPJ(valor);
        }

        public ModeloFornecedor CarregaModeloFornecedor(int codigo)
        {
            DalFornecedor DALobj = new DalFornecedor(conexao);
            return DALobj.CarregaModeloFornecedor(codigo);
        }

        public ModeloFornecedor CarregaModeloFornecedor(string cnpj)
        {
            DalFornecedor DALobj = new DalFornecedor(conexao);
            return DALobj.CarregaModeloFornecedor(cnpj);
        }
    }
}