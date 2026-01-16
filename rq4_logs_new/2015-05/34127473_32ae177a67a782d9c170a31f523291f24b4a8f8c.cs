using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cocarsa1.Entidades;
using MySql.Data.MySqlClient;

namespace Cocarsa1.ConexionBD
{
    class AbonoDao
    {
        public int registrarAbono(Abono pagoAbono, String tipoPago) 
        {
            int ans = 0;            
            String query = "";

            if (tipoPago.Equals("nota"))
                query = "INSERT INTO pagoAbonoNota VALUES(?idNota,?idCliente,?idCajera,?montoAbono,?fechaAbono)";
            else if(tipoPago.Equals("larguillo"))
                query = "INSERT INTO pagoAbonoLarguillo VAUES(?idLarguillo,?idCliente,?idCajera,?montoAbono,?fechaAbono)";

            Conexion conexion = new Conexion();
            MySqlCommand cmd = new MySqlCommand(query, conexion.abrirConexion());

            if(tipoPago.Equals("nota"))
                cmd.Parameters.AddWithValue("?idNota",pagoAbono.IdFolio);
            else if (tipoPago.Equals("larguillo"))
                cmd.Parameters.AddWithValue("?idLarguillo", pagoAbono.IdFolio);

            cmd.Parameters.AddWithValue("?idCliente", pagoAbono.IdCliente);
            cmd.Parameters.AddWithValue("?idCajera", pagoAbono.IdCajera);
            cmd.Parameters.AddWithValue("?montoAbono", pagoAbono.MontoAbono);
            cmd.Parameters.AddWithValue("?fechaAbono", pagoAbono.FechaAbono);

            ans = cmd.ExecuteNonQuery();
            return ans;
        }
    }
}