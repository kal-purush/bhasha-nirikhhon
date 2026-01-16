using Excepciones;
using LogicaNegocio.Dominio;
using LogicaNegocio.InterfacesRepositorios;
using SendGrid.Helpers.Mail;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LogicaAccesoDatos.BaseDatos
{
    public class RepositorioNotificaciones : IRepositorioNotificaciones
    {
        public CobrosContext Contexto { get; set; }

        public RepositorioNotificaciones(CobrosContext context)
        {
            Contexto = context;
        }


        public void Add(Notificacion obj)
        {
            try
            {

             Contexto.Add(obj);
             Contexto.SaveChanges();

            }
            catch (NotificacionException ex)
            {
                throw;
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public IEnumerable<Notificacion> FindAll()
        {
            throw new NotImplementedException();
        }

        public Notificacion FindById(int id)
        {
            throw new NotImplementedException();
        }

        public void Remove(int id)
        {
            throw new NotImplementedException();
        }

        public void Update(Notificacion obj)
        {
            throw new NotImplementedException();
        }
    }
}