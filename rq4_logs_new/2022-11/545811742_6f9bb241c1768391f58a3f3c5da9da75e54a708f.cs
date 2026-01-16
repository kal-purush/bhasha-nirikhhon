using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ActGlobant3
{
    internal class NuevosDatos
    {
        public bool Positivo { get; }
        public int Numero { get; set; }

        public NuevosDatos(int numero)
        {
            if (numero == 0) 
            {
                throw new Exception("El ramdom no puede ser 0 :D");
            }
            this.Numero = numero;
            Positivo = numero > 0;// de lo contrarion es false 
        }
    }
}