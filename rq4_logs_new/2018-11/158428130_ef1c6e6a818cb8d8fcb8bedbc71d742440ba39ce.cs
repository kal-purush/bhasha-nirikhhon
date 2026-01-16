using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Datos
{
    public class Configuracíon
    {
        public static string Conexion()
        {
            return ConfigurationManager.ConnectionStrings["myStrCon"].ConnectionString;
        }
    }
}