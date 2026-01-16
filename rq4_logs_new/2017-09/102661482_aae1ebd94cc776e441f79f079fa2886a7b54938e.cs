using System;
using System.Collections.Generic;
using System.Text;
using ConecxionDB.Dataset.DatasetConecxionTableAdapters;
using ConecxionDB.Modelo;

using System.Collections.Generic;
using System.Threading.Tasks;

using System;
namespace LogicaDeNegocio.Logic
{
  public  class LogicEmpleado
    {
        public void AgregarEmpleado( Empleado  empleado)
        {
            var x = new EmpleadoTableAdapter();
            x.InsertEmpleado(empleado.Nombre, empleado.Puesto);
        }

        public object llenarGrid()
        {
            var x = new EmpleadoTableAdapter();
           return x.GetDataByEmpleado();
        }
    }
}