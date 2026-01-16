using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Disney.Domain.Entities
{
    public class Character : EntityBase
    {
        [Required(ErrorMessage = "Ingrese una imagen")]
        [Url]
        [Column(TypeName = "VARCHAR")]
        public string urlImage { get; set; }

        [Required(ErrorMessage = "Nombre del personaje requerido")]
        [Column(TypeName = "VARCHAR(100)")]
        public string Name { get; set; }

        [Required(ErrorMessage = "Edad del personaje requerido")]
        [Column(TypeName = "INT")]
        public int Age { get; set; }

        [Required(ErrorMessage = "Peso del personaje requerido")]
        [Column(TypeName = "FLOAT(1,2)")]
        public double Weight { get; set; }

        [Required(ErrorMessage = "Ingrese la historia del personaje")]
        [Column(TypeName = "LONGTEXT")]
        public string History { get; set; }
        public virtual MovieSerie associatedMovieSerie { get; set; }
        public bool Status { get; set; }
    }
}