using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cocarsa1.Entidades;
using System.Windows.Forms;
using MySql.Data.MySqlClient;

namespace Cocarsa1.ConexionBD
{
    class LarguilloDao
    {

        public List<VentaLarguillo> cargarLarguillos(int idCliente) 
        {
            List<VentaLarguillo> lista = new List<VentaLarguillo>();
            
            Conexion conexion = new Conexion();

            try 
            {
                String query = "SELECT * FROM nota WHERE idCliente = ?idCliente";

                MySqlCommand cmd = new MySqlCommand(query, conexion.abrirConexion());
                cmd.Parameters.AddWithValue("?idCliente", idCliente);

                MySqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read()) { 
                    
                }

                conexion.cerrarConexion();
            } 
            catch(Exception e) {
                MessageBox.Show("Error al obtener acceso a la base de datos.");
            }

            return lista;    
        }
    }
}