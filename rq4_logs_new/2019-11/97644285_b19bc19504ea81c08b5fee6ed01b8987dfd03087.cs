using Intranet.Dashboard.Model;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Web;

namespace Intranet.Dashboard.Controller
{
    public class DashboardController
    {
        public List<DashboardProd> ListarRegistros(DateTime Fecha,string Maquina,int Procedimiento)
        {
            List<DashboardProd> lista = new List<DashboardProd>();
            Conexion con = new Conexion();
            SqlCommand cmd = con.AbrirConexionIntranet();
            if (cmd != null)
            {
                try
                {
                    cmd.CommandText = "DashboardProduccion";
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@Fecha", Fecha);
                    cmd.Parameters.AddWithValue("@Maquina", Maquina);
                    cmd.Parameters.AddWithValue("@Procedimiento", Procedimiento);
                    SqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        DashboardProd d = new DashboardProd();
                        d.Maquina = reader["CodRecurso"].ToString();
                        d.FechaProduccion = reader["DtProducao"].ToString();
                        d.TotalHoras = Convert.ToDouble(reader["HorasTotales"].ToString());
                        d.HorasTiraje= Convert.ToDouble(reader["HorasTiraje"].ToString());
                        d.HorasPreparacion= Convert.ToDouble(reader["HorasPreparacion"].ToString());
                        d.HorasDelay= Convert.ToDouble(reader["HorasDelay"].ToString());
                        d.HorasColacion= Convert.ToDouble(reader["HorasColacion"].ToString());
                        d.HorasAseo= Convert.ToDouble(reader["HorasAseos"].ToString());
                        d.HorasSinTrabajo= Convert.ToDouble(reader["HorasSinTrabajo"].ToString());
                        d.HorasMantencion= Convert.ToDouble(reader["HorasMantencion"].ToString());
                        d.Buenos= Convert.ToInt32(reader["Buenos"].ToString());
                        d.MalosPreparacion= Convert.ToInt32(reader["MalosPreparacion"].ToString());
                        d.MalosTiraje= Convert.ToInt32(reader["MalosTiraje"].ToString());
                        d.Preparaciones= Convert.ToInt32(reader["Preparaciones"].ToString());
                        d.Arranques= Convert.ToInt32(reader["Arranques"].ToString());
                        d.Preparaciones2 = Convert.ToInt32(reader["Preparaciones2"].ToString());
                        d.Arranques2 = Convert.ToInt32(reader["Arranques2"].ToString());
                        d.LogisticaMaterial= Convert.ToDouble(reader["LogisticaMaterial"].ToString());
                        d.Encuadernacion= Convert.ToDouble(reader["Encuadernacion"].ToString());
                        d.Impresion= Convert.ToDouble(reader["Impresion"].ToString());
                        d.Logistica= Convert.ToDouble(reader["Logistica"].ToString());
                        d.Mantencion= Convert.ToDouble(reader["Mantencion"].ToString());
                        d.Mecanico= Convert.ToDouble(reader["Mecanico"].ToString());
                        d.Electrico= Convert.ToDouble(reader["Electrico"].ToString());
                        d.Gestion= Convert.ToDouble(reader["Gestion"].ToString());
                        d.Material= Convert.ToDouble(reader["Material"].ToString());
                        d.Atascos= Convert.ToDouble(reader["Atascos"].ToString());
                        d.EsperaCambioTurno= Convert.ToDouble(reader["EsperaCambioTurno"].ToString());
                        d.ParadaPorJefatura= Convert.ToDouble(reader["ParadaPorJefatura"].ToString());
                        d.SinInformacion= Convert.ToDouble(reader["SinInformacion"].ToString());
                        d.Operacional= Convert.ToDouble(reader["Operacional"].ToString());
                        d.Planchas= Convert.ToDouble(reader["Planchas"].ToString());
                        d.Planificacion= Convert.ToDouble(reader["Planificacion"].ToString());
                        d.ServicioCliente= Convert.ToDouble(reader["ServicioCliente"].ToString());
                        d.RegulacionyLavados= Convert.ToDouble(reader["RegulacionyLavados"].ToString());
                        d.HorasSinPersonal=Convert.ToDouble(reader["HorasSinPersonal"].ToString());
                        d.HorasImprod_Prep= Convert.ToDouble(reader["Improd_Prep"].ToString());
                        lista.Add(d);
                    }
                }
                catch(Exception ex)
                {
                }
            }
            con.CerrarConexion();
            return lista;
        }
        public List<Recursos> ListarRecursos()
        {
            List<Recursos> lista = new List<Recursos>();
            Conexion con = new Conexion();
            SqlCommand cmd = con.AbrirConexionIntranet();
            if (cmd != null)
            {
                try
                {
                    cmd.CommandText = "DashboardProduccion";
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@Fecha", DateTime.Now);
                    cmd.Parameters.AddWithValue("@Maquina", "");
                    cmd.Parameters.AddWithValue("@Procedimiento", 3);
                    SqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        Recursos d = new Recursos();
                        d.Maquina = reader["Maquina"].ToString();
                        d.CodMaquina = reader["CodMaquina"].ToString();
                        d.Valor = Convert.ToInt32(reader["Valor"].ToString());
                        lista.Add(d);
                    }
                }
                catch (Exception ex)
                {
                }
            }
            con.CerrarConexion();
            return lista;
        }
    }
}