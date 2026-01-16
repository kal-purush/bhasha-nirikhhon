using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;

namespace MulticentroProyectoFinal
{
    class FacturacionAgregarBD : IAgregarElementoBD
    {
        //referencia a clase para abrir conexion
        private IConexionesBasicasAbrirCerrarBD conexion;
        //referencia a clase de Windows Form para poder capturar los valores ingresados por el cliente
        private FacturarCrear guiFacturacionesCrear = (FacturarCrear)Application.OpenForms["FacturarCrear"];
        //declaracion de propiedades Nombre, codigo, precio con su respectivo getter y setter
        public string fecha { get; set; }
        public string numFactura { get; set; }
        public string idCliente { get; set; }
        public string servicio { get; set; }
        private SqlCommand cmd;
        IBuscarElementoPorCodigoBD busqueda;
        public FacturacionAgregarBD()
        {

        }
        //Implementación de método de la interfaz

        /// <summary>
        /// método para agregar datos a servicios
        /// </summary>
        public void Agregar()
        {
            busqueda = new ServiciosBusquedaBD();
            conexion = new ConexionesBasicasAbrirCerrarBD();
            fecha = guiFacturacionesCrear.GetFecha();
            numFactura = guiFacturacionesCrear.GetNumFactura();
            idCliente = guiFacturacionesCrear.GetNombreCliente();
            servicio = guiFacturacionesCrear.GetServicio();
            try
            {
                if (VerificarInformacion())
                {
                    conexion.AbrirConexion();

                    cmd = new SqlCommand("insert into Multicentro.dbo.factura (facturaid,cliente_clienteid,fecha)" +
                        " values('" + int.Parse(numFactura) + "', '" + int.Parse(idCliente + "', '" + int.Parse(fecha)) + "')", conexion.GetSqlConnection());
                    cmd.ExecuteNonQuery();
                    conexion.CerrarConexion();

                    busqueda.BuscarPorCodigo(guiFacturacionesCrear.GetNumFactura(), guiFacturacionesCrear.GetDataView());
                    MessageBox.Show("Información agregada");
                }

            }
            catch (FormatException ex)
            {
                MensajesStandard.MensajeFormatoIncorrectoDatos();
            }
            catch (SqlException ex)
            {
                MensajesStandard.MensajeErrorGeneralBaseDatos();
            }
            catch (Exception ex)
            {
                MensajesStandard.MensajeGeneralExcepcionGenerica(ex);
            }
        }
        /// <summary>
        /// Método que verifica que no se hayan dejado en blanco nombre, código y precio
        /// </summary>
        /// <returns></returns>
        public bool VerificarInformacion()
        {
            if (fecha.Length == 0 || numFactura.Length == 0 || idCliente.Length == 0)
            {
                MessageBox.Show("No puede dejar espacios en blanco");
                return false;
            }
            return true;
        }

    }
}