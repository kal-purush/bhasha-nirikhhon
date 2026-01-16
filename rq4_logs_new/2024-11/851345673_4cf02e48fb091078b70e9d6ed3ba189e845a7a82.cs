using System;
using System.Collections.Generic;

namespace SIPT.BL.DTO.DTO
{
	public class PtuSolcertificadoinspectorDTO
	{
		public int? intcodsolicitudinspector { get; set; }
		public int? intusuinspector { get; set; }
		public int? intcodsolicitud { get; set; }
		public Int16? smlestado { get; set; }
		public string vchaudusucreacion { get; set; }
		public DateTime? tmsaudfeccreacion { get; set; }
		public string vchaudusumodif { get; set; }
		public DateTime? tmsaudfecmodif { get; set; }
		public string vchaudequipo { get; set; }
		public string vchaudprograma { get; set; }
	}
}