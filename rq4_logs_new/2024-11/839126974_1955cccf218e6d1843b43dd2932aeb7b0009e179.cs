using Aplication;
using Aplication.Interfaces;
using Aplication.Interfaces.Observer;
using Aplication.Services.Observer;
using Models;
using Models.Composite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Unity;

namespace GUI
{
    public partial class DesbloquearUsuario : System.Web.UI.Page, IIdiomaService
    {
        private readonly IUsuarioService _usuarioService;
        private readonly IPermisoService _permisoService;
        private readonly IdiomaService _idiomaService;

        public DesbloquearUsuario()
        {
            _usuarioService = Global.Container.Resolve<IUsuarioService>();
            _permisoService = Global.Container.Resolve<IPermisoService>();
            _idiomaService = Global.Container.Resolve<IdiomaService>();
            _idiomaService.Subscribe(this);
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            var usuario = Session["Usuario"] as Usuario;
            if (!_permisoService.TienePermiso(usuario, Permiso.DesbloquearUsuario))
            {
                Response.Redirect("AccesoDenegado.aspx");
                return;
            }

            if (!IsPostBack)
            {
                CargarUsuariosBloqueados();
                string selectedLanguage = Session["SelectedLanguage"] as string ?? "es";
                ddlLanguage.SelectedValue = selectedLanguage;
                _idiomaService.CurrentLanguage = selectedLanguage;
                CargarTextos();
            }
        }

        private void CargarTextos()
        {
            if (!(lblTituloDesbloqueo == null))
            {
                lblTituloDesbloqueo.Text = _idiomaService.GetTranslation("TituloDesbloquearUsuarios");
                btnDesbloquear.Text = _idiomaService.GetTranslation("ButtonDesbloquear");
                Page.Title = _idiomaService.GetTranslation("PageTitleDesbloquearUsuarios");

                gvUsuariosBloqueados.Columns[1].HeaderText = _idiomaService.GetTranslation("ID");
                gvUsuariosBloqueados.Columns[2].HeaderText = _idiomaService.GetTranslation("Nombre");
                gvUsuariosBloqueados.Columns[3].HeaderText = _idiomaService.GetTranslation("Apellido");
                gvUsuariosBloqueados.Columns[4].HeaderText = _idiomaService.GetTranslation("Email");
                gvUsuariosBloqueados.Columns[5].HeaderText = _idiomaService.GetTranslation("Estado");
                gvUsuariosBloqueados.EmptyDataText = _idiomaService.GetTranslation("MensajeNoUsuariosBloqueados");
                gvUsuariosBloqueados.DataBind();
                
                string script = $@"
                    var tituloConfirmacion = '{_idiomaService.GetTranslation("ConfirmacionDesbloqueo")}';
                    var mensajeConfirmacion = '{_idiomaService.GetTranslation("TextoConfirmacionDesbloqueo")}';
                    var botonConfirmar = '{_idiomaService.GetTranslation("BotonConfirmarDesbloqueo")}';
                    var botonCancelar = '{_idiomaService.GetTranslation("BotonCancelarDesbloqueo")}';
                    var mensajeSeleccionUsuarioError = '{_idiomaService.GetTranslation("MensajeSeleccionUsuarioError")}';
                ";
                ClientScript.RegisterStartupScript(this.GetType(), "Translations", script, true);
            }
        }

        protected void btnDesbloquear_Click(object sender, EventArgs e)
        {
            Usuario usuario = Session["Usuario"] as Usuario;
            bool usuarioDesbloqueado = false;

            foreach (GridViewRow row in gvUsuariosBloqueados.Rows)
            {
                CheckBox chkSeleccionar = (CheckBox)row.FindControl("chkSeleccionar");
                if (chkSeleccionar != null && chkSeleccionar.Checked)
                {

                    var email = EncriptacionService.Encriptar_AES(row.Cells[4].Text);

                    usuarioDesbloqueado = _usuarioService.DesbloquearUsuario(email, usuario, false);

                    if (usuarioDesbloqueado)
                    {
                        lblMensaje.Text = _idiomaService.GetTranslation("MensajeUsuarioDesbloqueadoExito");
                        lblMensaje.CssClass = "text-success";
                        lblMensaje.Visible = true;

                        CargarUsuariosBloqueados();
                    }
                    else
                    {
                        lblMensaje.Text = _idiomaService.GetTranslation("MensajeErrorDesbloqueo");
                        lblMensaje.CssClass = "text-danger";
                        lblMensaje.Visible = true;
                    }
                    break;
                }
            }

            if (!usuarioDesbloqueado)
            {
                lblMensaje.Text = _idiomaService.GetTranslation("MensajeSeleccionUsuarioError");
                lblMensaje.CssClass = "text-danger";
                lblMensaje.Visible = true;
            }
        }
        private void CargarUsuariosBloqueados()
        {
            List<Usuario> usuariosBloqueados = _usuarioService.ObtenerUsuariosBloqueados();
            gvUsuariosBloqueados.DataSource = usuariosBloqueados;
            gvUsuariosBloqueados.DataBind();

            if (usuariosBloqueados == null || usuariosBloqueados.Count == 0)
            {
                gvUsuariosBloqueados.EmptyDataText = _idiomaService.GetTranslation("MensajeNoUsuariosBloqueados");
            }
        }


        protected void btnCancelar_Click(object sender, EventArgs e)
        {

        }

        protected void chkSeleccionar_CheckedChanged(object sender, EventArgs e)
        {
            foreach (GridViewRow row in gvUsuariosBloqueados.Rows)
            {
                CheckBox chk = (CheckBox)row.FindControl("chkSeleccionar");
                if (chk != (CheckBox)sender)
                {
                    chk.Checked = false;
                }
            }
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
            Response.Redirect(Request.RawUrl);
        }
    }
}