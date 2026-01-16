using Aplicada2ProyectoFinal.Data;
using Aplicada2ProyectoFinal.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Aplicada2ProyectoFinal.Controllers
{
    public class TipoClientesController
    {
        public bool Guardar(TiposClientes TipoCliente)
        {
            Contexto contexto = new Contexto();
            bool paso = false;
            try
            {
                if (TipoCliente.TipoClienteId == 0)
                {
                    paso = Insertar(TipoCliente);
                }
                else
                {
                    paso = Modificar(TipoCliente);
                }
            }
            catch (Exception)
            {
                throw;
            }
            return paso;
        }
        private bool Insertar(TiposClientes TipoCliente)
        {
            Contexto contexto = new Contexto();
            bool paso = false;
            try
            {
                contexto.TiposClientes.Add(TipoCliente);
                paso = contexto.SaveChanges() > 0;
            }
            catch (Exception)
            {
                throw;
            }
            return paso;
        }
        private bool Modificar(TiposClientes TipoCliente)
        {
            Contexto contexto = new Contexto();
            bool paso = false;
            try
            {
                contexto.TiposClientes.Add(TipoCliente);
                contexto.Entry(TipoCliente).State = EntityState.Modified;
                paso = contexto.SaveChanges() > 0;
            }
            catch (Exception)
            {
                throw;
            }
            return paso;
        }

        public TiposClientes Buscar(int id)
        {
            Contexto contexto = new Contexto();
            TiposClientes TipoCliente = new TiposClientes();
            try
            {
                TipoCliente = contexto.TiposClientes.Find(id);
            }
            catch (Exception)
            {
                throw;
            }
            return TipoCliente;
        }
        public bool Eliminar(int id)
        {
            Contexto contexto = new Contexto();
            bool paso = false;
            TiposClientes TipoCliente = new TiposClientes();

            try
            {
                TipoCliente = contexto.TiposClientes.Find(id);
                contexto.Entry(TipoCliente).State = EntityState.Deleted;
                paso = contexto.SaveChanges() > 0;
            }
            catch (Exception)
            {
                throw;
            }
            return paso;
        }
        public List<TiposClientes> GetList(Expression<Func<TiposClientes, bool>> expression)
        {
            Contexto contexto = new Contexto();
            List<TiposClientes> lista;
            try
            {
                lista = contexto.TiposClientes.Where(expression).ToList();
            }
            catch (Exception)
            {
                throw;
            }
            return lista;
        }
    }
}