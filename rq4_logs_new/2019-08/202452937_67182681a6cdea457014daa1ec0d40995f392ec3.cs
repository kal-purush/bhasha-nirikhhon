using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace API_Asada.Models
{
    public class Cheque
    {
        [Key]
        public int Id { get; set; }
        [ForeignKey("Cuenta_Bancaria")]
        public int Id_cuenta { get; set; }
        public virtual Cuenta_Bancaria Cuenta_Bancaria { get; set; }
        public DateTime fecha { get; set; }
        [StringLength(100)]
        public String numero_cheque { get; set; }
        [StringLength(200)]
        public String numero_cuenta { get; set; }
        public Double monto { get; set; }
        [StringLength(50, ErrorMessage = "El nombre del portador no puede excerder los 50 caracteres")]
        public String portador { get; set; }
        [Column(TypeName = "text")]
        public String concepto { get; set; }
        public Boolean tipo { get; set; }
        [StringLength(250)]
        public String cuenta_contable { get; set; }
    }
}