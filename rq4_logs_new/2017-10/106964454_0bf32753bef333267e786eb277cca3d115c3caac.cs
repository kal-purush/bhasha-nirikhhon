using LocadoraCarro.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace LocadoraCarro.DAO
{
    public class ClasseDAO
    {
        public void Adiciona(Classe classe)
        {
            using (var context = new LocadoraContext())
            {
                context.Classes.Add(classe);
                context.SaveChanges();
            }
        }

        public void Remove(Classe classe)
        {
            using (var contexto = new LocadoraContext())
            {
                contexto.Entry(classe).State = System.Data.Entity.EntityState.Deleted;
                contexto.SaveChanges();
            }
        }

        public IList<Classe> Lista()
        {
            using (var contexto = new LocadoraContext())
            {
                return contexto.Classes.ToList();
            }
        }

        public Classe BuscaPorId(int id)
        {
            using (var contexto = new LocadoraContext())
            {
                return contexto.Classes.Find(id);
            }
        }

        public void Atualiza(Classe classe)
        {
            using (var contexto = new LocadoraContext())
            {
                contexto.Entry(classe).State = System.Data.Entity.EntityState.Modified;
                contexto.SaveChanges();
            }
        }
    }
}