using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WaterSolutionAPI.Models;

namespace WaterSolutionAPI.ModelDTO
{
	public class EmpleadoDTO
	{
		public string nombreUsuario { get; set; }
		public string nombreEmpleado { get; set; }
		public string ApellidosEmpleado { get; set; }
		public string cedulaEmpleado { get; set; }
		public DateTime fechaEmpleado { get; set; }
		public string TelefornoEmpleado { get; set; }
		public string DireccionEmpleado { get; set; }
		public string nombreSeccion { get; set; }
		public string nombreDepartamento { get; set; }
		public string nombreCargo { get; set; }
		public string nombreRole { get; set; }
		public List<PermisoRole> Permiso { get; set; }
	}
}