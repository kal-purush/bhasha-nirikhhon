using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cocarsa1.Entidades;
using MySql.Data.MySqlClient;

namespace Cocarsa1.ConexionBD
{
    class ProductoDao
    {
        public int obtenerIdProducto() {
            int ans = -1;

            Conexion bd = new Conexion();
            String query = "SELECT idProducto FROM producto ORDER BY idProducto desc limit 1";

            MySqlCommand cmd = new MySqlCommand(query, bd.abrirConexion());
            ans = cmd.ExecuteNonQuery();
            bd.cerrarConexion();

            return ans;
        }
        
        public ProductoE obtenerProducto(int idProducto) {
            ProductoE producto = null;

            Conexion bd = new Conexion();
            String query = "SELECT * FROM producto WHERE idProducto = ?idProducto";

            MySqlCommand cmd = new MySqlCommand(query, bd.abrirConexion());
            cmd.Parameters.AddWithValue("?idProducto", idProducto);

            MySqlDataReader reader = cmd.ExecuteReader();
            if(reader.Read()) 
            {
                producto = new ProductoE();
                producto.IdProducto = reader.GetInt32("idProducto");
                producto.Nombre = reader.GetString("nombre");
                producto.PrecioVenta = reader.GetDouble("precioVenta");
            }

            return producto;
        }

        public int registrarProducto(ProductoE producto) {
            int ans = -1;

            Conexion bd = new Conexion();
            String query = "INSERT INTO producto(idProducto, nombre, precioVenta) VALUES(?idProducto, ?nombre, ?precioVenta)";

            MySqlCommand cmd = new MySqlCommand(query, bd.abrirConexion());
            cmd.Parameters.AddWithValue("idProducto", producto.IdProducto);
            cmd.Parameters.AddWithValue("nombre", producto.Nombre);
            cmd.Parameters.AddWithValue("precioVenta", producto.PrecioVenta);

            ans = cmd.ExecuteNonQuery();

            return ans;
        }
    }
}