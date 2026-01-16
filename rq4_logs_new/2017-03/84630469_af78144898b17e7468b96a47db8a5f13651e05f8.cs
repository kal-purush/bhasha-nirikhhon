using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.Data;

namespace paqueteriaSQLSERVER
{
    class ConexionSQL
    {
        private SqlCommand cmd;
        private SqlConnection c;
        private SqlDataAdapter data;
        private DataTable dt;

        private string cadenaConexion = "Data Source=.;Initial Catalog=Paqueteria;Integrated Security=True";

        public ConexionSQL()
        {
            c = new SqlConnection(cadenaConexion);
        }


        private bool Open()
        {
            try
            {
                if(c.State != ConnectionState.Open)
                {
                    c.Open();
                }
            }
            catch(Exception e)
            {
                MessageBox.Show("Error en la conexion de la base de datos");
                return false;
            }
            return true;
        }


        private void Close()
        {
            try
            {
                c.Close();
            }
            catch(Exception e)
            {

            }
        }

        /// <summary>
        /// LLena un Datagrid con el Resultado de una Consulta SQL
        /// </summary>
        /// <param name="query">Query a ejecutar (No debe tener errores)</param>
        /// <param name="dgv">Datagrid donde se vaciara el resultado de la Consulta </param>
        public void actualizaDataGridView(string query, DataGridView dgv)
        {

            try
            {
                Open();
                cmd = new SqlCommand(query, c);
                data = new SqlDataAdapter(cmd);
                dt = new DataTable();
                data.Fill(dt);
                dgv.DataSource = dt;
                Close();

            }
            catch(Exception e)
            {
                //MessageBox.Show("Error en la ejecucion 0x00ffffff");
            }
        }

        /// <summary>
        /// Ejecuta una Instrucion SQL
        /// </summary>
        /// <param name="sql"> query que sera ejecutado por SQL ( No debe Contener errores )</param>
        public void EjecutaSQL(string sql)
        {
            try
            {
                Open();
                cmd = new SqlCommand(sql, c);
                cmd.ExecuteNonQuery();
                MessageBox.Show("Ejecucion Correcta");
                Close();

            }
            catch(SqlException e)
            {
                MessageBox.Show("Error 0x00ffdddd \n " + e);
            }
        }
        /// <summary>
        /// Actualiza un combo Box Con datos traidos de la base de datos 
        /// </summary>
        /// <param name="sql"> Consulta sql que regresara elementos para el sql </param>
        /// <param name="cb"> Combobox donde se vaciaran los resultados obtenidos por el query </param>
        public void ActualizaComboBox(string sql, ComboBox cb)
        {

        }


    }
}