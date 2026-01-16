using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Model;
using Persistence;

namespace Services
{
    public class GerenciadorEnvio : IGerenciadorEnvio
    {
        private readonly bancocollectorContext _context;
        public GerenciadorEnvio(bancocollectorContext context)
        {
            _context = context;
        }
        public void Editar(Envio envioModel)
        {
            try
            {
                Tbenvio tbenvio = new Tbenvio();
                Atribuir(envioModel, tbenvio);
                _context.Update(tbenvio);
                _context.SaveChanges();
            }
            catch (Exception e)
            {
                throw new Exception($"Envio não atualizado. Erro de {e.Message}");
            }
        }

        private void Atribuir(Envio envioModel, Tbenvio tbenvio)
        {
            try
            {
                tbenvio.CodEnvio = envioModel.CodEnvio;
                tbenvio.CodNegociacao = envioModel.CodNegociacao;
                tbenvio.Destinatario = envioModel.Destinatario;
                tbenvio.Complemento = envioModel.Complemento;
                tbenvio.Endereco = envioModel.Endereco;
                tbenvio.Remetente = envioModel.Remetente;
                tbenvio.Tipo = envioModel.Tipo;
                tbenvio.Valor = envioModel.Valor;
                tbenvio.Bairro = envioModel.Bairro;     
            }
            catch (Exception e)
            {
                throw new Exception($"Dados não atribuidos. Erro {e.Message}");
            }
        }

        public int Inserir(Envio envioModel)
        {
            Tbenvio _envio = new Tbenvio();
            _envio.CodEnvio = envioModel.CodEnvio;
            _envio.CodNegociacao = envioModel.CodNegociacao;
            _envio.Destinatario = envioModel.Destinatario;
            _envio.Endereco = envioModel.Endereco;
            _envio.Bairro = envioModel.Bairro;
            _envio.Remetente = envioModel.Remetente;
            _envio.Tipo = envioModel.Tipo;
            _envio.Valor = envioModel.Valor;
            _context.Add(_envio);
            _context.SaveChanges();
            return _envio.CodEnvio;
        }

        public Envio Obter(int idEnvio)
        {
            IEnumerable<Envio> envios = GetQuery().Where(envioModel => envioModel.CodEnvio.Equals(idEnvio));
            return envios.ElementAtOrDefault(0);
        }

        private IEnumerable<Envio> GetQuery()
        {
            IQueryable<Tbenvio> Tbenvio = _context.Tbenvio;
            var query = from envio in Tbenvio
                        select new Envio
                        {
                            CodEnvio = envio.CodEnvio,
                            Destinatario = envio.Destinatario,
                            Endereco = envio.Endereco,
                            Remetente = envio.Remetente,
                            Bairro = envio.Bairro,
                            Valor = envio.Valor,
                            Tipo = envio.Tipo,
                            Complemento = envio.Complemento,
                            CodNegociacao=envio.CodNegociacao
                        };
            return query;
        }

        public IEnumerable<Envio> ObterPorNomeDestinario(string nome)
        {
            IEnumerable<Envio> subcategorias = GetQuery().Where(envioModel => envioModel.Destinatario.StartsWith(nome));
            return subcategorias;
        }

        public IEnumerable<Envio> ObterTodos()
        {
            return GetQuery();
        }

        public void Remover(int idEnvio)
        {
            var tbenvio = _context.Tbsubcategoria.Find(idEnvio);
            _context.Tbsubcategoria.Remove(tbenvio);
            _context.SaveChanges();
        }
    }
}