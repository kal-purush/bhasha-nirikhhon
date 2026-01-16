using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace API_Asada.Models
{
    public class Compra_Inventario
    {
        [Key]
        public int Id { get; set; }
        [ForeignKey("Registro_Compra")]
        public int Id_compra { get; set; }
        public virtual Registro_Compra Registro_Compra { get; set; }
        [ForeignKey("Inventario_Stock")]
        public int Id_articulo { get; set; }
        public virtual Inventario_Stock Inventario_Stock { get; set; }
        public int cantidad { get; set; }
        public DateTime fecha { get; set; }

    }
}