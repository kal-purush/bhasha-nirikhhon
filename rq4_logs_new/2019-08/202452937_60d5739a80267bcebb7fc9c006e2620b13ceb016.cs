using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace API_Asada.Models
{
    public class Averia
    {
        [Key]
        public int Id { get; set; }
        [ForeignKey("Asada_Queja")]
        public int Id_queja { get; set; }
        public virtual Asada_Queja Asada_Queja { get; set; }
        public int numero_averia { get; set; }
        public DateTime fecha { get; set; }
        [StringLength(10)]
        public String numero_casa { get; set; }
        [StringLength(150)]
        public String numero_medidor { get; set; }
        [StringLength(150)]
        public String responsable { get; set; }
        [Column(TypeName = "text")]
        public String observaciones { get; set; }
        public DateTime fecha_trabajo { get; set; }
        public Double costo_mano_obra { get; set; }
    }
}