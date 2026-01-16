using DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;


namespace CAD
{
    public class CADDireccion
    {
        static string cadena = ConfigurationManager.ConnectionStrings["conSQLServer"].ConnectionString;
        static SqlConnection con = new SqlConnection(cadena);
        static SqlCommand cmd;

        public void guardarDireccion(DTODireccion dir)
        {
            SqlCommand cmd = new SqlCommand(); // sentencias sql
            cmd.Connection = con;
            cmd.CommandText = "prc_GuardarDireccion";
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@cod_web", dir.Codigo);
            cmd.Parameters.AddWithValue("@dir_web", dir.Direccion);
            con.Open();


            cmd.ExecuteNonQuery();
            //cerrar conexiòn
            con.Close();
            //
        }
    }



}