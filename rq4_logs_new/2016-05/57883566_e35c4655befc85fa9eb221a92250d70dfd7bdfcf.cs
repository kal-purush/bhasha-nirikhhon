using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServidorRMI
{
    public interface ICalculoFGTS
    {
        double Calcula(double salario, int meses);
    }
}