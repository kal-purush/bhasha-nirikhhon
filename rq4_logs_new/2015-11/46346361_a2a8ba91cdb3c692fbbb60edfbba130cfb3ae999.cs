using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Data;
using System.Windows.Forms;
//#####  CLASE QUE CONTIENE FUNCIONES RELACIONADAS CON LA BASE DE DATOS  #####

namespace EnterpriseSolution
{
    class ClassConexion
    {
        private string _cadena;
        private MySqlConnection _conexion;
        private DataTable _tabla = new DataTable();
        private DataTable _tabla2 = new DataTable();
        private MySqlDataAdapter _adaptador = new MySqlDataAdapter();
        private ClassMensajeError MsjErr = new ClassMensajeError();
        private int _filas;

        //#####  CREA LA CADENA DE CONEXION A LA BASE DE DATOS  #####
        private void conexion()
        {
            bool estado = false;
            ConnectionStringSettings MiCnx;
            try
            {
                MiCnx = ConfigurationManager.ConnectionStrings["EnterpriseSolution.Properties.Settings.bdnominaConnectionString"];
                _cadena = MiCnx.ConnectionString;
                //MessageBox.Show(_cadena);
                _conexion = new MySqlConnection(_cadena);
            }
            catch (Exception ex)
            {
                MsjErr.MsjBox(2, "ERR004-07", ex.ToString());
            }
        }

        //#####  REALIZA UN CONSULTA (SELECT) A LA BASE DE DATOS  #####
        public bool consulta(string script)
        {
            bool estado = true;
            try
            {
                _tabla.Clear();
                conexion();
                _adaptador.SelectCommand = new MySqlCommand(script, _conexion);
                _adaptador.Fill(_tabla);
            }
            catch (Exception ex)
            {
                MsjErr.MsjBox(2, "ERR004-01", ex.ToString());
                if (ex.ToString().Contains("Unable to connect to any of the specified MySQL hosts"))
                {
                    Application.ExitThread();
                    Environment.Exit(0);
                }
                estado = false;
            }
            return estado;
        }
        //#####  REALIZA UN CONSULTA (SELECT) A LA BASE DE DATOS  #####
        public bool consulta2(string script)
        {
            bool estado = true;
            try
            {
                _tabla2.Clear();
                conexion();
                _adaptador.SelectCommand = new MySqlCommand(script, _conexion);
                _adaptador.Fill(_tabla2);
            }
            catch (Exception ex)
            {
                MsjErr.MsjBox(2, "ERR004-01", ex.ToString());
                if (ex.ToString().Contains("Unable to connect to any of the specified MySQL hosts"))
                {
                    Application.ExitThread();
                    Environment.Exit(0);
                }
                estado = false;
            }
            return estado;
        }

        //#####  RETORNA LOS DATOS DE LA CONSULTA  #####
        public DataTable datos()
        {
            return _tabla;
        }

        //#####  CARGA LA INFORMACION DE LOS COMBOBOX  #####

        public void CargarComboBox(string cons, DevExpress.XtraEditors.ComboBoxEdit cbx, string Col)
        {
            try
            {
                conexion();
                cbx.Properties.Items.Clear();
                if (consulta2(cons))
                {
                    if (_tabla2.Rows.Count > 0)
                    {
                        for (int i = 0; i < _tabla2.Rows.Count; i++)
                        {
                            cbx.Properties.Items.Add(_tabla2.Rows[i][Col].ToString());
                        }
                    }
                    _tabla2.Clear();
                }
                _tabla2.Dispose();
            }
            catch (Exception ex)
            {
                MsjErr.MsjBox(7, "ERRGEN-02", ex.ToString());
                Application.ExitThread();
                Environment.Exit(0);
            }

        }



    }
}