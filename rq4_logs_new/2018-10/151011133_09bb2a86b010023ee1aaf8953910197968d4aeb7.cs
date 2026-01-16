using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entrega2_POO
{
    class Especial : Naturaleza
    {
        protected override int Attack(int ataqueBase, Habilidad habilidad)
        {
            return 1;
        }

        protected override void Habilility()
        {

        }
    }
}