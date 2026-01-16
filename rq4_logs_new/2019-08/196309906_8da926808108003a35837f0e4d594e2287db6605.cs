using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace MulticentroProyectoFinal
{ 
    class FacturacionJoinBD
{
    private IConexionesBasicasAbrirCerrarBD conexion = new ConexionesBasicasAbrirCerrarBD();
    private SqlCommand comando = new SqlCommand();
    private SqlDataReader lector;

    public DataTable ProductosTable()
    {
        DataTable tablaproductos = new DataTable();
            comando.Connection = conexion.GetSqlConnection();
            conexion.AbrirConexion();
        comando.CommandText = "listaProductos";
        comando.CommandType = CommandType.StoredProcedure;
        lector = comando.ExecuteReader();
        tablaproductos.Load(lector);
        lector.Close();
        conexion.CerrarConexion();
        return tablaproductos;

    }

    public DataTable ServiciosTable()
    {
        DataTable tablaServicios = new DataTable();
            comando.Connection = conexion.GetSqlConnection();
           
        comando.CommandText = "listaServicios";
        comando.CommandType = CommandType.StoredProcedure;
        lector = comando.ExecuteReader();
        tablaServicios.Load(lector);
        lector.Close();
            conexion.CerrarConexion();
        return tablaServicios;

    }
}
}