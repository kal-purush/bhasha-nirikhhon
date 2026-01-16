using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EntidadPropiedades
{
    public class Alumnos
    {
		public int Id { get; set; }
		public string Nombres { get; set; }
		public string Apellidos { get; set; }
		public string Sexo { get; set; }
		public string EstadoCivil { get; set; }
		public string Telefono { get; set; }
		public string Direccion { get; set; }
		public bool Activo { get; set; }

		public Alumnos()
		{
			Activo = true;
		}

	}
}