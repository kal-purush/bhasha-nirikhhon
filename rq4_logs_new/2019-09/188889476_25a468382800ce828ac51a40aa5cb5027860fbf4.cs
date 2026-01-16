using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Model;
using Persistence;

namespace Services
{
    public class ServiceNegociacaoAnuncio : IServiceNegociacaoAnuncio

    {
        public bancocollectorContext _Contexto { get; private set; }

        public ServiceNegociacaoAnuncio(bancocollectorContext _contexto)
        {
            _Contexto = _contexto;
        }

        public void Editar(NegociacaoAnuncio negociacaoAnuncio)
        {
            var entidade = new Negociacaoanuncio();
            Transferir(negociacaoAnuncio, entidade);
            _Contexto.Entry(entidade);
            _Contexto.Entry(entidade).State = Microsoft.EntityFrameworkCore.EntityState.Modified;
            _Contexto.SaveChanges();


        }

        private IQueryable<NegociacaoAnuncio> GetQuery()
        {
            IQueryable<Negociacaoanuncio> tb_negociacaoanuncio = _Contexto.Negociacaoanuncio;
            var query = from x in tb_negociacaoanuncio
                        select new NegociacaoAnuncio
                        {
                            CodAnuncio = x.CodAnuncioNavigation.CodAnuncio,
                            CodNegociacao = x.CodNegociacaoNavigation.CodUsuario

                        };
            return query;

        }
        public (int, int) Inserir(NegociacaoAnuncio negociacaoAnuncio)
        {
            var entidadeParaTabela = new Negociacaoanuncio();
            Transferir(negociacaoAnuncio, entidadeParaTabela);
            _Contexto.Negociacaoanuncio.Add(entidadeParaTabela);

            _Contexto.SaveChanges();
            return (negociacaoAnuncio.CodAnuncio, negociacaoAnuncio.CodNegociacao);
        }

        public NegociacaoAnuncio Obter(int codNegociacao, int codAnuncio)
        {

            IEnumerable<NegociacaoAnuncio> negociacoesanuncios = GetQuery()
                .Where(negoanuncio => negoanuncio.CodAnuncio.Equals(codAnuncio)
                && negoanuncio.CodNegociacao.Equals(codNegociacao));
            return negociacoesanuncios.ElementAtOrDefault(0);
        }

        public IEnumerable<NegociacaoAnuncio> ObterTodos() => _Contexto.Negociacaoanuncio.
            Select(x => new NegociacaoAnuncio
            {
                CodAnuncio = x.CodAnuncioNavigation.CodAnuncio,
                CodNegociacao = x.CodNegociacaoNavigation.CodNegociacao
            }).ToList();

        public void Remover(int codNegociacao, int codAnuncio)
        {
            var tbnegociacaoAnuncio01 = _Contexto.Negociacaoanuncio.Find(codNegociacao);
            var tbnegociacaoAnuncio02 = _Contexto.Negociacaoanuncio.Find(codAnuncio);
            _Contexto.Negociacaoanuncio.Remove(tbnegociacaoAnuncio01);
            _Contexto.Negociacaoanuncio.Remove(tbnegociacaoAnuncio02);
            _Contexto.SaveChanges();


        }
        private void Transferir(NegociacaoAnuncio x, Negociacaoanuncio y)
        {
            y.CodAnuncioNavigation.CodAnuncio = x.CodAnuncio;
            y.CodNegociacaoNavigation.CodNegociacao = x.CodNegociacao;
        }
    }
}