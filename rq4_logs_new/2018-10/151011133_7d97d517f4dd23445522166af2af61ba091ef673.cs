using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Diagnostics;

namespace Entrega2_POO
{
    class PoderTrueno
    {
        List<string> nombrePoderes;
        List<int> danoPoderes;
        List<int> HabilidadPoderes;

        public string getPoder(int i)
        {
            return nombrePoderes[i];
        }

        public int Ataque(int i, int ataqueBase)
        {
            if (i == 1)
            {
                return danoPoderes[i] + ataqueBase;
            }
            return danoPoderes[i] * ataqueBase;
            
        }
    }
}