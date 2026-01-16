using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace SysVenda.Domain.Entidades
{
    [Table("TELEFONES")]
    public class Telefone
    {
        [Key]
        public int Codigo { get; set; }

        public string Ddd { get; set; }

        public string Numero { get; set; }

        public int PessoaCd { get; set; }

        [ForeignKey("PessoaCd")]
        public Pessoa Pessoa { get; set; }
    }
}