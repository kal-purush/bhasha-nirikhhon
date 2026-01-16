using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Datos
{
    public class CategoriaDAL
    {


        public DataTable listar()
        {
            Conexion objConexion = new Conexion();
            return objConexion.LeerPorComando("select * from Categoria");
        }
        





    }
}