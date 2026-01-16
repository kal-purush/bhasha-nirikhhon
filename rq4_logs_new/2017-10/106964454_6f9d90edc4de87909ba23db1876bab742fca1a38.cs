using LocadoraCarro.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace LocadoraCarro.DAO
{
    public class ModeloDAO
    {
        public void Adiciona(Modelo modelo)
        {
            using (var context = new LocadoraContext())
            {
                context.Modelos.Add(modelo);
                context.SaveChanges();
            }
        }

        public IList<Modelo> Lista()
        {
            using (var contexto = new LocadoraContext())
            {
                return contexto.Modelos.ToList();
            }
        }

        public Modelo BuscaPorId(int id)
        {
            using (var contexto = new LocadoraContext())
            {
                return contexto.Modelos.Find(id);
            }
        }

        public void Atualiza(Modelo modelo)
        {
            using (var contexto = new LocadoraContext())
            {
                contexto.Entry(modelo).State = System.Data.Entity.EntityState.Modified;
                contexto.SaveChanges();
            }
        }
    }
}