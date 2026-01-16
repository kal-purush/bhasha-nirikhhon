using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Gestao_de_Projetos.Models
{
    public class Contacto
    {

        private const string sms = "Introduza um email valido !";

        public int contactoID { get; set; }

        public string nome { get; set; }
        public string sobrenome { get; set; }

        public string email { get; set; }

        public string mensagem { get; set; }
        public string assunto { get; set; }

        public bool verificado { get; set; }

        public string resposta { get; set; }

    }
}