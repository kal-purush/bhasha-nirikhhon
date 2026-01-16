using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;
using System.Globalization;
using MulticentroProyectoFinal.GUI;

namespace MulticentroProyectoFinal.Facturacion
{
    class AgregarServiciosYProductosFactura
    {

        //referencia a clase para abrir conexion
        private IConexionesBasicasAbrirCerrarBD conexion;
        private FacturarCrear factura= (FacturarCrear)Application.OpenForms["FacturarCrear"];
        private FacturacionProducto facturacionProducto = (FacturacionProducto)Application.OpenForms["FacturacionProducto"];
        private FacturacionServicio facturacionServicio = (FacturacionServicio)Application.OpenForms["FacturacionServicio"];
        public int Detalleid { get; set; }
        int numero = 0;
        public int Cantidad { get; set; }
        public int Codigo_Servicio { get; set; }
        public int Producto_codigoproducto { get; set; }
        public int Precio { get; set; }
        public int Factura_facturaid { get; set; }
        private SqlCommand cmd;
        IBuscarElementoPorCodigoYNombre busqueda;
        public bool Agregar()
        {
            busqueda = new ServiciosBusquedaBD();
            conexion = new ConexionesBasicasAbrirCerrarBD();

            Detalleid = numero + 1; ;
            Cantidad = Int32.Parse(facturacionProducto.getCantidad());
            Precio = 5000;
            Producto_codigoproducto = Int32.Parse(facturacionProducto.GetCodigo());
            //Codigo_Servicio = Int32.Parse(facturacionServicio.GetCodigo());
            Factura_facturaid = Int32.Parse(factura.GetNumFactura());
            try
            {              
                    conexion.AbrirConexion();

                    cmd = new SqlCommand("insert into Multicentro.dbo.detalle (detalleid,cantidad,producto_codigoproducto, precio,factura_facturaid)" +
                        " values('" + Detalleid + "', '" + Cantidad + "',  '" + Producto_codigoproducto + "', '" + Precio + "', '" + Factura_facturaid + "')", conexion.GetSqlConnection());
                    cmd.ExecuteNonQuery();
                    conexion.CerrarConexion();

                  busqueda.BuscarPorCodigo(Detalleid.ToString(), facturacionProducto.GetDataView());
                    MessageBox.Show("Información agregada");
                    return true;
                

            }

            catch (FormatException ex)
            {
                MensajesStandard.MensajeFormatoIncorrectoDatos();
                return false;
            }
            catch (SqlException ex)
            {
                MensajesStandard.MensajeErrorGeneralBaseDatos();
                return false;
            }
            catch (Exception ex)
            {
                MensajesStandard.MensajeGeneralExcepcionGenerica(ex);
                return false;
            }
            return false;
        }
        /// <summary>
        /// Método que verifica que no se hayan dejado en blanco nombre, código y precio
        /// </summary>
        /// <returns></returns>
        //public bool VerificarInformacion()
        //{
        //    if (Nombre.Length == 0 || Codigo.Length == 0 || Precio.Length == 0)
        //    {
        //        MessageBox.Show("No puede dejar espacios en blanco");
        //        return false;
        //    }
        //    return true;
        //}

    }
}

