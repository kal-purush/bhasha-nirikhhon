using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PrototipoModeloPlataforma.Modelos
{
    public class Ticket
    {
        [Key]
        public int id { get; set; }
        [Required]
        public string tipo { get; set; }
        [Required]
        public string descripcion { get; set; }
        [Required]
        public int idregistro { get; set; }
    }
}