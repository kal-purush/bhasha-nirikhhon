using LogicaNegocio.InterfacesDominio;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace LogicaNegocio
{
    public class Usuario: IValidacion
    {
        public int Id { get; set; }
        public string Email { get; set; }

        [MinLength(8)]
        public string Password { get; set; }

        public void ValidarDatosLogin()
        {
            ValidarEmail();
            ValidarPassword();

        }

        public void ValidarEmail()
        {
            if (Email != null && !Email.Contains("@"))
            {
                throw new Exception("mail invalido");
            }
        }

        private void ValidarPassword()
        {
            
        }

        
    }
}