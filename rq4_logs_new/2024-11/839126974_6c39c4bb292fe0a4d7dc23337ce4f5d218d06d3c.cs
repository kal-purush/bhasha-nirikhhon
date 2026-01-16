using Aplication.Interfaces;
using Aplication.Interfaces.Observer;
using Aplication.Services.Observer;
using Models;
using Models.Composite;
using System;
using System.Web.UI;
using System.Web.UI.WebControls;
using Unity;

namespace GUI
{
    public partial class MiCuenta : Page, IIdiomaService
    {
        private readonly IUsuarioService _iUsuarioService;
        private readonly IPermisoService _permisoService;
        private readonly IdiomaService _idiomaService;

        public MiCuenta()
        {
            _iUsuarioService = Global.Container.Resolve<IUsuarioService>();
            _permisoService = Global.Container.Resolve<IPermisoService>();
            _idiomaService = Global.Container.Resolve<IdiomaService>();
            _idiomaService.Subscribe(this);
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            var usuario = Session["Usuario"] as Usuario;
            if(!_permisoService.TienePermiso(usuario, Permiso.MisDatos))
            {
                Response.Redirect("AccesoDenegado.aspx");
                return;
            }

            try
            {
                if (!IsPostBack)
                {
                    string selectedLanguage = Session["SelectedLanguage"] as string ?? "es";
                    ddlLanguage.SelectedValue = selectedLanguage;
                    _idiomaService.CurrentLanguage = selectedLanguage;
                    CargarGenero();
                    CargarTextos();   
                    CargarUsuario();
                }
            }
            catch (Exception ex)
            {
                lblMensaje.CssClass = "text-danger";
                lblMensaje.Visible = true;
                lblMensaje.Text = _idiomaService.GetTranslation(ex.Message);
            }
            finally
            {
                if (!IsPostBack)
                    CargarTextos();
            }
        }

        protected void btnGuardar_Click(object sender, EventArgs e)
        {
            try
            {
                Validar();

                var userSession = Session["Usuario"] as Usuario;

                var usuario = CompletarUsuario(userSession);

                _iUsuarioService.ModificarUsuario(usuario, userSession);
            }
            catch (Exception ex)
            {
                lblMensaje.CssClass = "text-danger";
                lblMensaje.Visible = true;
                lblMensaje.Text = _idiomaService.GetTranslation(ex.Message);
            }
        }

        protected void btnCancelarModificacion_Click(object sender, EventArgs e)
        {
            CargarUsuario();
        }

        private Usuario CompletarUsuario(Usuario userSession)
        {
            var usuario = new Usuario()
            {
                Id = userSession.Id,
                Email = userSession.Email,
                Puesto = userSession.Puesto,
                Area = userSession.Area,
                Nombre = txtNombre.Text,
                Apellido = txtApellido.Text,
                Genero = drpGenero.Text,
                Direccion = txtDireccion.Text,
                NumeroDireccion = int.Parse(txtNumeroDireccion.Text),
                Departamento = txtDepartamento.Text,
                CodigoPostal = txtCodigoPostal.Text,
                Ciudad = txtCiudad.Text,
                Provincia = txtProvincia.Text,
                Pais = txtPais.Text
            };

            return usuario;
        }

        private void CargarUsuario()
        {
            var userSession = Session["Usuario"] as Usuario;
            var usuario = _iUsuarioService.ObtenerUsuarioPorId(userSession.Id);

            if(usuario == null)
            {
                throw new Exception("MensajeErrorGeneral");
            }
            else
            {
                txtApellido.Text = usuario.Apellido;
                txtCiudad.Text = usuario.Ciudad;
                txtCodigoPostal.Text = usuario.CodigoPostal;
                txtDepartamento.Text = usuario.Departamento;
                txtDireccion.Text = usuario.Direccion;
                txtNombre.Text = usuario.Nombre;
                txtNumeroDireccion.Text = usuario.NumeroDireccion.ToString();
                txtPais.Text = usuario.Pais;
                txtProvincia.Text = usuario.Provincia;

                var item = drpGenero.Items.FindByText(usuario.Genero);
                if (item != null)
                {
                    drpGenero.SelectedValue = item.Value;
                }
            }
        }

        private void CargarTextos()
        {
            if (!(lblMensaje == null))
            {
                lblMensaje.Text = _idiomaService.GetTranslation("MensajeNoResultados");
                litFormTitle.Text = _idiomaService.GetTranslation("MisDatos");
                btnGuardar.Text = _idiomaService.GetTranslation("Guardar");
                btnCancelarModificacion.Text = _idiomaService.GetTranslation("Cancelar");
                litNombreLabel.Text = _idiomaService.GetTranslation("Nombre");
                litApellidoLabel.Text = _idiomaService.GetTranslation("Apellido");
                litDireccionLabel.Text = _idiomaService.GetTranslation("Direccion");
                litNumeroDireccionLabel.Text = _idiomaService.GetTranslation("NumeroDireccion");
                litDepartamentoLabel.Text = _idiomaService.GetTranslation("Departamento");
                litCodigoPostalLabel.Text = _idiomaService.GetTranslation("CodigoPostal");
                litCiudadLabel.Text = _idiomaService.GetTranslation("Ciudad");
                litProvinciaLabel.Text = _idiomaService.GetTranslation("Provincia");
                litPaisLabel.Text = _idiomaService.GetTranslation("Pais");
                lblGenero.Text = _idiomaService.GetTranslation("LabelGenero");
            }
        }

        private void CargarGenero()
        {
            drpGenero.Items.Clear();
            drpGenero.Items.Add(new ListItem("Seleccione un genero", "0"));
            drpGenero.Items.Add(new ListItem("Masculino", "1"));
            drpGenero.Items.Add(new ListItem("Femenino", "2"));
            drpGenero.Items.Add(new ListItem("No Especifica", "3"));
        }

        private string Validar()
        {
            if (string.IsNullOrEmpty(txtApellido.Text) || string.IsNullOrEmpty(txtCiudad.Text) || string.IsNullOrEmpty(txtCodigoPostal.Text) 
                || string.IsNullOrEmpty(txtDepartamento.Text) || string.IsNullOrEmpty(txtDireccion.Text)  || string.IsNullOrEmpty(txtNombre.Text)
                || string.IsNullOrEmpty(txtNumeroDireccion.Text) || string.IsNullOrEmpty(txtPais.Text) || string.IsNullOrEmpty(txtProvincia.Text))
                throw new Exception(_idiomaService.GetTranslation("MensajeCamposIncompletos"));

            return null;
        }

        public void UpdateLanguage(string language)
        {
            CargarTextos();
        }

        protected override void OnUnload(EventArgs e)
        {
            _idiomaService.Unsubscribe(this);
            base.OnUnload(e);
        }

        protected void ddlLanguage_SelectedIndexChanged(object sender, EventArgs e)
        {
            string selectedLanguage = ddlLanguage.SelectedValue;
            Session["SelectedLanguage"] = selectedLanguage;
            _idiomaService.CurrentLanguage = selectedLanguage;
            CargarTextos();
            Response.Redirect(Request.RawUrl);
        }
    }
}