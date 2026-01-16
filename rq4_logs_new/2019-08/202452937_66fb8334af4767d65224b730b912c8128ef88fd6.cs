using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace API_Asada.Models
{
    public class Inventario_Stock
    {
        [Key]
        public int Id { get; set; }
        [StringLength(50)]
        public String codigo_articulo { get; set; }
        [StringLength(200)]
        public String nombre_articulo { get; set; }
        public int cantidad { get; set; }
    }
}