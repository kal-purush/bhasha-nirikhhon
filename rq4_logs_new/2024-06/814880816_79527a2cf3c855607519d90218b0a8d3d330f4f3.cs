using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IntegradorBB
{
    internal class Conexion
    {
        //Conexion a la BdD
        public static MySqlConnection conexion()
        {
            // Variables de conexion
            string servidor = "localhost";
            string bd = "tienda";
            string usuario = "root";
            string pass = "200603";
            string sentenciaSql = "Server=" + servidor + ";Database=" + bd + ";User Id=" + usuario + ";Password=" + pass + ";";

            try { 
                MySqlConnection BDconexion = new MySqlConnection(sentenciaSql);
                return BDconexion;
            }catch (MySqlException e) { 
                Console.WriteLine("Error: " + e.Message);
                return null;
            }
        }


    }
}