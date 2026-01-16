using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace API_Asada.Models
{
    public class Reciclaje
    {
        [Key]
        public int Id { get; set; }
        [ForeignKey("Material")]
        public int Fk_material { get; set; }
        public int cantidad { get; set; }
        public Double precio_kilo { get; set; }
        public DateTime fecha { get; set; }
        public int numero_boleta { get; set; }
    }
}