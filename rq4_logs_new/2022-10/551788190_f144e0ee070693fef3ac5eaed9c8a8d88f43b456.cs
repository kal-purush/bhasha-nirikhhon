using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace princeshop.Models
{
    [Table("t_proforma")]
    public class Proforma
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Column("id")]
        public int Id { get; set; }   
        public String? UserID { get; set; }
        public Producto? Producto { get; set; }
        public string? Talla {get;set;}
        public int Cantidad { get; set; }

        public Decimal Precio { get; set; }
        public String Status { get; set; } ="PENDIENTE";
    }
}