using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TP_Integrador
{
    internal class PlazoFijo
    {
        private decimal monto;
        private DateTime fechaInicio;
        private int duracion;

        public PlazoFijo (decimal monto, int duracion)
        {
            this.monto = monto;
            fechaInicio = DateTime.Now;
            this.duracion = duracion;
        }   
    }
}