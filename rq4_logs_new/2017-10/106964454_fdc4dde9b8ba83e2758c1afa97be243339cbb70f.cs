using LocadoraCarro.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace LocadoraCarro.DAO
{
    public class AluguelDAO
    {
        public void Adiciona(Aluguel aluguel)
        {
            using (var context = new LocadoraContext())
            {
                context.Alugueis.Add(aluguel);
                context.SaveChanges();
            }
        }

        public void Remove(Aluguel aluguel)
        {
            using (var contexto = new LocadoraContext())
            {
                contexto.Entry(aluguel).State = System.Data.Entity.EntityState.Deleted;
                contexto.SaveChanges();
            }
        }

        public IList<Aluguel> Lista()
        {
            using (var contexto = new LocadoraContext())
            {
                return contexto.Alugueis.ToList();
            }
        }

        public Aluguel BuscaPorId(int id)
        {
            using (var contexto = new LocadoraContext())
            {
                return contexto.Alugueis.Find(id);
            }
        }

        public void Atualiza(Aluguel aluguel)
        {
            using (var contexto = new LocadoraContext())
            {
                contexto.Entry(aluguel).State = System.Data.Entity.EntityState.Modified;
                contexto.SaveChanges();
            }
        }
    }
}