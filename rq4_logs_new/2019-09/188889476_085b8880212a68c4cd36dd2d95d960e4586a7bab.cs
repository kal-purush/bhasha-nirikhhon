using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Model;
using Persistence;

namespace Services
{
    public class GerenciadorPagamento : IGerenciadorPagamento
    {
        private readonly bancocollectorContext _context;
        public GerenciadorPagamento(bancocollectorContext context)
        {
            _context = context;
        }
        public void Editar(Pagamento pagamentoModel)
        {
            try
            {
                Tbpagamento tbpagamento = new Tbpagamento();
                Atribuir(pagamentoModel, tbpagamento);
                _context.Update(tbpagamento);
                _context.SaveChanges();
            }
            catch (Exception e)
            {
                throw new Exception($"Pagamento não atualizado. Erro de {e.Message}");
            }
        }

        private void Atribuir(Pagamento pagamentoModel, Tbpagamento tbpagamento)
        {
            try
            {
                tbpagamento.CodPagamento = pagamentoModel.CodPagamento;
                tbpagamento.Opcao = pagamentoModel.Opcao;
                tbpagamento.Valor = pagamentoModel.Valor;

            }
            catch (Exception e)
            {
                throw new Exception($"Dados não atribuidos. Erro {e.Message}");
            }
        }

        public int Inserir(Pagamento pagamentoModel)
        {
            Tbpagamento _pagamento = new Tbpagamento();
            _pagamento.CodPagamento = pagamentoModel.CodPagamento;
            _pagamento.Opcao = pagamentoModel.Opcao;
            _pagamento.Valor = pagamentoModel.Valor;
            _context.Add(_pagamento);
            _context.SaveChanges();
            return _pagamento.CodPagamento;
        }

        public Pagamento Obter(int codPagamento)
        {
            IEnumerable<Pagamento> pagamentos = GetQuery().Where(pagamentoModel => pagamentoModel.CodPagamento.Equals(codPagamento));
            //CodPagamento.Equals(codPagamento));
            return pagamentos.ElementAtOrDefault(0);
        }

        private IEnumerable<Pagamento> GetQuery()
        {
            IQueryable<Tbpagamento> Tbpagamento = _context.Tbpagamento;
            var query = from pagamento in Tbpagamento
                        select new Pagamento
                        {
                            CodPagamento = pagamento.CodPagamento,
                            Opcao = pagamento.Opcao,
                            Valor = pagamento.Valor,
                         //   CodNegociação = pagamento.Negociacaopagamento.Where( )
                        };
            return query;
        }

        public IEnumerable<Pagamento> ObterTodos()
        {  
                return GetQuery();
        }

        public void Remover(int idPagamento)
        {
            var tbPagamento = _context.Tbpagamento.Find(idPagamento);
            _context.Tbpagamento.Remove(tbPagamento);
            _context.SaveChanges();
        }
    }
}