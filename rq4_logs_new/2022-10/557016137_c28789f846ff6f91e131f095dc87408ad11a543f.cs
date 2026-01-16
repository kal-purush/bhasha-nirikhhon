using System.ComponentModel.DataAnnotations;
using System;

namespace ASPNET_Centralizador.DTO
{
    public class EstudianteUpdateDTO
    {
        [Key]
        public int ci { get; set; }
        [Required]
        [MaxLength(50)]
        public string nombre { get; set; }
        [Required]
        [MaxLength(50)]
        public string apellido { get; set; }
        [Required]
        public DateTime fecha_nac { get; set; }
        [MaxLength(100)]
        public string email { get; set; }
        [MaxLength(60)]
        public string direccion { get; set; }
    }
}