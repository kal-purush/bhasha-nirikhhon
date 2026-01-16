using HeroisDoCancer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace HeroisDoCancer.Repositorios
{
    public class EventoRepositorio
    {
        private IContextoDados contexto;

        public EventoRepositorio(IContextoDados contexto)
        {
            if (contexto == null)
                throw new ArgumentNullException("contexto");

            this.contexto = contexto;
        }

        public Evento[] ObterTodosConfirmados()
        {
            return this.contexto.Eventos
                .Where(e => e.TipoSituacao == TipoSituacaoEnum.Confirmado)
                .OrderByDescending(e => e.CriadoEm)
                .ToArray();
        }
    }
}