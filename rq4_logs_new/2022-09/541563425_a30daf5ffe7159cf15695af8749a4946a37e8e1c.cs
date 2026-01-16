using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PONGAdocoes.Models
{
    internal class Adocoes
    {
        public int Id { get; set; }
        public DateTime DataAdocao { get; set; }

        public Adocoes ()
        {
            DataAdocao = DateTime.Now;
        }
    }
}