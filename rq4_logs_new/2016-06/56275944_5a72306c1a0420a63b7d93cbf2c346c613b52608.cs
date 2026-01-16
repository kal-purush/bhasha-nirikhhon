using SuperERP.Compras.DTO;
using SuperERP.DAL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SuperERP.Compras.Service
{
    class FuncionalidadeService
    {
        public static void Cadastrar(FuncionalidadeDTO funcionalidade)
        {
            ServicoGenerico<Funcionalidade, FuncionalidadeDTO>.Cadastrar(funcionalidade);
        }
        public static void Alterar(FuncionalidadeDTO funcionalidade)
        {
            ServicoGenerico<Funcionalidade, FuncionalidadeDTO>.Alterar(funcionalidade);
        }
        public static ICollection<FuncionalidadeDTO> Listar()
        {
            return ServicoGenerico<Funcionalidade, FuncionalidadeDTO>.Listar();
        }
        public static void Deletar(int IdFuncionalidade)
        {
            ServicoGenerico<Funcionalidade, FuncionalidadeDTO>.Deletar(IdFuncionalidade);
        }
        public static FuncionalidadeDTO BuscaPorId(int IdFuncionalidade)
        {
            return ServicoGenerico<Funcionalidade, FuncionalidadeDTO>.BuscaPorId(IdFuncionalidade);
        }
    }
}