using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BibliotecaProyecto
{
    public class CambioCargo
    {
        public DateTime fecha { get; set; }
        public Empleado empleado { get; set; }
        public Cargo cargoAnterior { get; set; }
        public Cargo cargoNuevo { get; set; }
        public double nuevoSalario { get; set; }

        public static List<CambioCargo> ListaHistoricoCambioCargos = new List<CambioCargo>();

        public static void AgregarHistoricoCambioCargo(CambioCargo h)
        {
            Empleado empleado = h.empleado;
            empleado.cargo = h.cargoNuevo;
            empleado.salarioBase = h.nuevoSalario;

            ListaHistoricoCambioCargos.Add(h);
        }

        public static  List<CambioCargo> ObtenerHistoricoCambioCargos ()
        {
            return ListaHistoricoCambioCargos;
        }

        public override string ToString()
        {
            return empleado.nombres;
        }
    }
}