using System;

namespace Hospital.App.Dominio

{
    public class Diagnostico
    {
        public int Id {get;set;}
        public DateTime FechaDiagnostico {get;set;}
        public string DiagnosticoMedico {get;set;}
        public int PacienteId {get; set;}
        public int HistClinId {get; set;}
        public List<SugerenciasCuidado> SugerenciasCuidado{ get; set; }
    }
}