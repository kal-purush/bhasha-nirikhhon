using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cocarsa1.Entidades;
using Cocarsa1.ConexionBD;
using MySql.Data.MySqlClient;

namespace Cocarsa1.ConexionBD
{
    class FajillasDAO
    {
        MySqlDataReader consulta;
        public Fajilla fajillaDia(DateTime fechaRegistro)
        {
            Fajilla fajillaDiaT = new Fajilla();
            MySqlConnection conn;
            Conexion conexion = new Conexion();
            conn = conexion.abrirConexion();
            String query = "SELECT idCajera,monto FROM `cocarsa`.`fajilla` WHERE fecha = current_date();";
            try
            {
                MySqlCommand cmd = new MySqlCommand(query, conn);
                cmd.Parameters.AddWithValue("current_date()", fechaRegistro);
                consulta = cmd.ExecuteReader();
                cmd.Dispose();
                while (consulta.Read())
                {
                    fajillaDiaT.IdCajera = consulta.GetInt32(0);
                    fajillaDiaT.Monto = consulta.GetDouble(1);
                }
            }
            
            finally
            {
                conexion.cerrarConexion();
            }
            return fajillaDiaT;
    
        }


        public Fajilla fajillaCaja(DateTime fechaRegistro) {
            Fajilla fajillaCaja = new Fajilla();
            MySqlConnection conn;
            Conexion conexion = new Conexion();
            conn = conexion.abrirConexion();
            String query = "SELECT idCajera,monto,fecha FROM `cocarsa`.`fajilla` WHERE enCaja = '1'";
            try
            {
                MySqlCommand cmd = new MySqlCommand(query, conn);
                cmd.Parameters.AddWithValue("current_date()", fechaRegistro);
                consulta = cmd.ExecuteReader();
                cmd.Dispose();
                while (consulta.Read())
                {
                    fajillaCaja.IdCajera = consulta.GetInt32(0);
                    fajillaCaja.Monto = consulta.GetDouble(1);
                    fajillaCaja.FechaRegistro = consulta.GetDateTime(2);
                }
            }

            finally
            {
                conexion.cerrarConexion();
            }


            return fajillaCaja;
        }



        /*** Cargar Fajilla del Dia**/
















    
    }
}