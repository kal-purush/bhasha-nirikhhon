using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace API_Asada.Models
{
    public class Factura_Cheque
    {
        [Key]
        public int Id { get; set; }
        [ForeignKey("Cheque")]
        public int Id_cheque { get; set; }
        public virtual Cheque Cheque { get; set; }
        [ForeignKey("Factura")]
        public int Id_factura { get; set; }
        public virtual Factura Factura { get; set; }

    }
}