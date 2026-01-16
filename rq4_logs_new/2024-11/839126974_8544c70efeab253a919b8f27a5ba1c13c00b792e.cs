using System;
using System.Web.UI;

namespace GUI.Controls
{
    public partial class ValidarRegistroUsuarioDatos : UserControl
    {
        public string Direccion
        {
            get { return txtDireccion.Text; }
            set { txtDireccion.Text = value; }
        }

        public int NumeroDireccion
        {
            get
            {
                int numero;
                int.TryParse(txtNumeroDireccion.Text, out numero);
                return numero;
            }
            set { txtNumeroDireccion.Text = value.ToString(); }
        }

        public string Departamento
        {
            get { return txtDepartamento.Text; }
            set { txtDepartamento.Text = value; }
        }

        public string CodigoPostal
        {
            get { return txtCodigoPostal.Text; }
            set { txtCodigoPostal.Text = value; }
        }

        public string Ciudad
        {
            get { return txtCiudad.Text; }
            set { txtCiudad.Text = value; }
        }

        public string Provincia
        {
            get { return txtProvincia.Text; }
            set { txtProvincia.Text = value; }
        }

        public string Pais
        {
            get { return txtPais.Text; }
            set { txtPais.Text = value; }
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            // Cualquier código de carga de página específico del control
        }

        public void Limpiar()
        {
            Direccion = string.Empty;
            NumeroDireccion = 0;
            Departamento = string.Empty;
            CodigoPostal = string.Empty;
            Ciudad = string.Empty;
            Provincia = string.Empty;
            Pais = string.Empty;
        }
    }
}