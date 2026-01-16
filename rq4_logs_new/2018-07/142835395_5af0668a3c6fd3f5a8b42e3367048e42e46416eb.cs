using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Datos
{
    public class D_Usuario
    {

        private Conexión conn  = new Conexión();
        private SqlDataReader leer;

        public D_Usuario()
        {
            
        }




        //Iniciar sesion normal
        public SqlDataReader iniciarSesion(string user, string pass)
        {
            string sql = "Select * from Usuarios where cedúla ='"+user+"' and contraseña ='"+pass+"'";
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = (conn.abrirConexión());
            cmd.CommandText = sql;
            
            leer = cmd.ExecuteReader();
            return leer;
        }

        //Iniciar sesion con procedimientos almacenados
        public SqlDataReader iniciarSesionP(string user, string pass)
        {
            SqlCommand cmd = new SqlCommand("SPIniciarSesion",conn.abrirConexión());
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@user",user);
            cmd.Parameters.AddWithValue("@pass",pass);

            leer = cmd.ExecuteReader();


            return leer;
        }
    }
}