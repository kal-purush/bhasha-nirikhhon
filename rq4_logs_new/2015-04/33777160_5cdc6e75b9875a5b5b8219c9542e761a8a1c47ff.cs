using HeroisDoCancer.Models;
using System;
using System.Linq;

namespace HeroisDoCancer.Repositorios
{
    public class HospitalRepositorio
    {
        private IContextoDados contexto;

        public HospitalRepositorio(IContextoDados contexto)
        {
            if (contexto == null)
                throw new ArgumentNullException("contexto");

            this.contexto = contexto;
        }

        public Hospital GetById(Int32 id)
        {
            return this.contexto.Hospitais
                .SingleOrDefault(e => e.Id == id);
        }

        public Hospital[] ObterTodos()
        {
            return this.contexto.Hospitais
                .OrderByDescending(e => e.Nome)
                .ToArray();
        }
    }
}