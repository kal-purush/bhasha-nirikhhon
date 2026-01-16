using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
namespace Laboratorio
{
    class clasConexion
    {
        MySqlConnection Conexion = new MySqlConnection();
        String sConexion;
        public void funiniciarConexion()
        {
            try{
                sConexion = "Server=localhost; Database=laboratorio; Uid=root; Pwd=;";
                Conexion.ConnectionString = sConexion;
                Conexion.Open();
                MessageBox.Show("Conexion establecida","Exito");
            }catch(MySqlException)
            {
                MessageBox.Show("Error en la conexion","ERROR");
            }

        }
        
    }
}