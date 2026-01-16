using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AutenticacionCoordenadas.Entities
{
    public class Usuario
    {
        public int Id { get; set; }
        public string usuario { get; set; }
        public string Identificacion { get; set; }
        public string NombreUsuario { get; set; }
        public string PrimerApellido { get; set; }
        public string SegundoApellido { get; set; }
        public int TipoAutenticacionId { get; set; }
        public string Correo { get; set; }
        public int CantidadIntentosAcceso { get; set; }
        public char TipoUsuario { get; set; }
        public string UsuarioActualiza { get; set; }
        public DateTime FechaActualiza { get; set; }
        public string Observaciones { get; set; }
        public bool Eliminado { get; set; }
        public int TipoIdentificacionID { get; set; }
        public int InstitucionID { get; set; }
        public int OficinaID { get; set; }
        public bool Activo { get; set; }



    }

}