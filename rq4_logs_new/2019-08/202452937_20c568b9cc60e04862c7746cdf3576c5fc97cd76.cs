using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;


namespace API_Asada.Models
{
    public class users
    {
        [Key]
        public int Id { get; set; }
        public String first_name { get; set; }
        public String last_name { get; set; }
        [StringLength(50, ErrorMessage = "El campo nombre de usuario no puede exceder los 50 caracteres"), Required]
        public String username { get; set; }
        [StringLength(100), Required, EmailAddress]
        public String email { get; set; }
        [StringLength(200),Required]
        public String password { get; set; }
        public Boolean IsAdmin { get; set; }
    }
}