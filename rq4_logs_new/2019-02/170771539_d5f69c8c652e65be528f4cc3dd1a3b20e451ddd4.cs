using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace SysVenda.Domain.Entidades
{
    [Table("CLASSIFICACOES_PESSOAS")]
    public class ClassificacaoPessoa
    {
        [Key]
        public int Codigo { get; set; }       

        [Column(TypeName = "varchar(30)")]
        public string Descricao { get; set; }

        public int PessoaCd { get; set; }

        [ForeignKey("PessoaCd")]
        public Pessoa Pessoa { get; set; }
    }
}