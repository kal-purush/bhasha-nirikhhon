using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entidades;

namespace DAL
{
    public class LoginAuditoriaDAL
    {
        private string cn;

        public LoginAuditoriaDAL(string cs) 
        {
            this.cn = cs;
        }

        public void RegistrarUsuarioLogin(string nombreUsuario)
        {
            using (DB_AcmeEntities contexto = new DB_AcmeEntities())
            {
                contexto.InsertarUsuarioLogin(nombreUsuario);
                contexto.SaveChanges();
            }
        }
    }
}