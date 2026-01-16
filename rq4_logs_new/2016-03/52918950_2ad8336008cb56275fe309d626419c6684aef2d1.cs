using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Navegador;

namespace Aerolinea
{
    public partial class frmPrincipalPonderacionNota : Form
    {
        public frmPrincipalPonderacionNota()
        {
            InitializeComponent();
            funActualizarGrid();
        }

        private void funActualizarGrid()
        {
            clasnegocio cnegocio = new clasnegocio();
            cnegocio.funconsultarRegistros("paquete", "SELECT ccodigo_paquete as No, t2.nombre as Curso, t3.nombre_salon as Salon, t4.rangoHora as Horario, t5.nombre as Seccion FROM paquete AS t1 INNER JOIN curso as t2 ON t1.codigo_curso = t2.codigo_curso INNER JOIN salon as t3 ON t1.codigo_salon = t3.codigo_salon INNER JOIN horario as t4 ON t1.codigohorario = t4.codigohorario INNER JOIN seccion as t5 ON t1.codigo_seccion = t5.codigo_seccion WHERE t1.estado = 'ACTIVO' AND t1.condicion = '1' ", "consulta", grdPaquete);
        }

        private void textBox1_KeyUp(object sender, KeyEventArgs e)
        {
            clasnegocio cnegocio = new clasnegocio();
            cnegocio.funconsultarRegistros("paquete", "SELECT ccodigo_paquete as No, t2.nombre as Curso, t3.nombre_salon as Salon, t4.rangoHora as Horario, t5.nombre as Seccion FROM paquete AS t1 INNER JOIN curso as t2 ON t1.codigo_curso = t2.codigo_curso INNER JOIN salon as t3 ON t1.codigo_salon = t3.codigo_salon INNER JOIN horario as t4 ON t1.codigohorario = t4.codigohorario INNER JOIN seccion as t5 ON t1.codigo_seccion = t5.codigo_seccion WHERE t1.estado = 'ACTIVO' AND t1.condicion = '1' AND t2.nombre LIKE '" + txtBuscar.Text + "%'", "consulta", grdPaquete);
        }

        private void btnRefrescar_Click(object sender, EventArgs e)
        {
            funActualizarGrid();
        }

        private void btnIrPrimero_Click(object sender, EventArgs e)
        {
            clasnegocio cnegocio = new clasnegocio();
            cnegocio.funPrimero(grdPaquete);
        }

        private void btnAnterior_Click(object sender, EventArgs e)
        {
            clasnegocio cnegocio = new clasnegocio();
            cnegocio.funAnterior(grdPaquete);
        }

        private void btnSiguiente_Click(object sender, EventArgs e)
        {
            clasnegocio cnegocio = new clasnegocio();
            cnegocio.funSiguiente(grdPaquete);
        }

        private void btnIrUltimo_Click(object sender, EventArgs e)
        {
            clasnegocio cnegocio = new clasnegocio();
            cnegocio.funUltimo(grdPaquete);
        }

        private void grdPaquete_CellContentDoubleClick(object sender, DataGridViewCellEventArgs e)
        {
            string sCodPaquete = grdPaquete.Rows[grdPaquete.CurrentCell.RowIndex].Cells[0].Value.ToString();
            /*string sNombre = grdPaquete.Rows[grdPaquete.CurrentCell.RowIndex].Cells[1].Value.ToString();
            string sSalon = grdPaquete.Rows[grdPaquete.CurrentCell.RowIndex].Cells[2].Value.ToString();
            string sHorario = grdPaquete.Rows[grdPaquete.CurrentCell.RowIndex].Cells[3].Value.ToString();
            string sSeccion = grdPaquete.Rows[grdPaquete.CurrentCell.RowIndex].Cells[4].Value.ToString();*/
            frmPonderacionNota temp = new frmPonderacionNota(sCodPaquete/*, sNombre, sSalon, sHorario, sSeccion*/);
            temp.Show();
        }
        
    }
}