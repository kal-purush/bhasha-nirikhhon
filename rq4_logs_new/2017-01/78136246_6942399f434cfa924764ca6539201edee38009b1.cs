using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;


namespace ArchivoPlanoAfiliacionBCP
{
    class Program
    {
        static void Main(string[] args)
        {
            string detalles = "";
            string rubro = "";
            string empresa = "";
            string servicio = "";
            string fecha = DateTime.Today.ToString("yyyyMMdd");
            string solicitud = "";
            string Ruta = ConfigurationManager.AppSettings["Ruta"];
            string IdMedioPago = ConfigurationManager.AppSettings["IdMedioPago"];
            string con = ConfigurationManager.ConnectionStrings["pexConnection"].ConnectionString;
            SqlDataAdapter da = new SqlDataAdapter();
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            SqlConnection SQLConn = new SqlConnection(con);
            SqlCommand SQLCmd = new SqlCommand();
            try
            {
                SQLCmd.Connection = SQLConn;
                SQLCmd.CommandType = CommandType.StoredProcedure;
                SQLCmd.CommandText = "GetArchivoPlanoAfiliacionDebitoAutomatico";
                SQLCmd.Parameters.Add("@IdMedioPago", SqlDbType.Int).Value = IdMedioPago;
                SQLConn.Open();
                da.SelectCommand = SQLCmd;
                dt.TableName = "debito";
                ds.Tables.Add(dt);
                da.Fill(dt);
                if (ds.Tables["debito"].Rows.Count > 0)
                {
                    rubro = ds.Tables["debito"].Rows[0][1].ToString();
                    empresa = ds.Tables["debito"].Rows[0][2].ToString();
                    servicio = ds.Tables["debito"].Rows[0][3].ToString();
                    solicitud = ds.Tables["debito"].Rows[0][4].ToString();
                    using (StreamWriter writer = new StreamWriter(Ruta + "T" + rubro + empresa + servicio + "_" + fecha + "_" + solicitud + ".014"))
                    {
                        for (int i = 0; i < ds.Tables["debito"].Rows.Count; i++)
                        {
                            detalles = ds.Tables["debito"].Rows[i][0].ToString().PadRight(2); //Tipo de Registro
                            detalles = detalles + ds.Tables["debito"].Rows[i][1].ToString().PadRight(2);//Código de Rubro
                            detalles = detalles + ds.Tables["debito"].Rows[i][2].ToString().PadRight(3);//Código de Empresa
                            detalles = detalles + ds.Tables["debito"].Rows[i][3].ToString().PadRight(2);//servicio
                            detalles = detalles + ds.Tables["debito"].Rows[i][4].ToString().PadRight(10);//codigo
                            detalles = detalles + ds.Tables["debito"].Rows[i][5].ToString().PadRight(20);//Operacion
                            detalles = detalles + ds.Tables["debito"].Rows[i][6].ToString().PadRight(8); //Fecha
                            detalles = detalles + ds.Tables["debito"].Rows[i][7].ToString().PadRight(6); //Hora
                            detalles = detalles + ds.Tables["debito"].Rows[i][8].ToString().PadRight(1);
                            detalles = detalles + ds.Tables["debito"].Rows[i][9].ToString().PadRight(11);
                            detalles = detalles + ds.Tables["debito"].Rows[i][10].ToString().PadRight(1);
                            detalles = detalles + ds.Tables["debito"].Rows[i][11].ToString().PadRight(3);
                            detalles = detalles + ds.Tables["debito"].Rows[i][12].ToString().PadRight(2);
                            detalles = detalles + ds.Tables["debito"].Rows[i][13].ToString().PadRight(20);
                            detalles = detalles + ds.Tables["debito"].Rows[i][14].ToString().PadRight(1);
                            detalles = detalles + ds.Tables["debito"].Rows[i][15].ToString().PadRight(3);
                            detalles = detalles + ds.Tables["debito"].Rows[i][16].ToString().PadRight(2);
                            detalles = detalles + ds.Tables["debito"].Rows[i][17].ToString().PadRight(20);
                            detalles = detalles + ds.Tables["debito"].Rows[i][18].ToString().PadRight(17);
                            detalles = detalles + ds.Tables["debito"].Rows[i][19].ToString().PadRight(36);
                            detalles = detalles + ds.Tables["debito"].Rows[i][20].ToString().PadRight(4);
                            writer.WriteLine(detalles);
                        }
                        writer.Close();
                    }
                }
                ds.Dispose();  
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message);
            }
            finally
            {
                Console.WriteLine("Ejecutando finalmente el bloque.");
                SQLConn.Close();
            }
        }
    }
}