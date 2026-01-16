using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace Web2ProjekatBackend.Models
{
    [Table("Ekipe")]
    public class Ekipa
    {

        [Key]
        public string IdEkipe { get; set; }
        [Required]
        public string NazivEkipe { get; set; }


        public Ekipa() { }
        public Ekipa(string idEkipe, string naziv)
        {

            this.IdEkipe = idEkipe;
            this.NazivEkipe = naziv;


        }

    }
}