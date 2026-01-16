using SuperERP.Compras.DTO;
using SuperERP.DAL.Models;
using SuperERP.Vendas.DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AutoMapper;

namespace SuperERP.Compras.Service
{
    public class ProdutoService
    {
        public static void Cadastrar(ProdutoDTO produto)
        {
            ServicoGenerico<Produto, ProdutoDTO>.Cadastrar(produto);
        }
        public static void Alterar(ProdutoDTO produto)
        {
            ServicoGenerico<Produto, ProdutoDTO>.Alterar(produto);
        }
        public static ICollection<ProdutoDTO> Listar()
        {
            return ServicoGenerico<Produto, ProdutoDTO>.Listar();
        }
        public static void Deletar(int IDProduto)
        {
            ServicoGenerico<Produto, ProdutoDTO>.Deletar(IDProduto);
        }
    }
}