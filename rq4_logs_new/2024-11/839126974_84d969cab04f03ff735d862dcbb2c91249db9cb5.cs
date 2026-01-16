using Aplication.Interfaces;
using Models;
using Models.Composite;
using System;
using System.Collections.Generic;
using System.Web.UI;
using System.Web.UI.WebControls;
using Unity;

namespace GUI
{
    public partial class MisDocumentos : Page
    {
        private readonly IDocumentoService _documentoService;
        private readonly IPermisoService _permisoService;

        public MisDocumentos()
        {
            _documentoService = Global.Container.Resolve<IDocumentoService>();
            _permisoService = Global.Container.Resolve<IPermisoService>();
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            var usuario = Session["Usuario"] as Usuario;
            if(!_permisoService.TienePermiso(usuario, Permiso.MisDocumentos))
            {
                Response.Redirect("AccesoDenegado.aspx");
                return;
            }

            if (!IsPostBack)
            {
                if (Session["DocumentosLeidos"] == null)
                {
                    Session["DocumentosLeidos"] = new HashSet<int>();
                }
                CargarDocumentos(false);
            }
        }

        private void CargarDocumentos(bool firmado)
        {
            var userSession = Session["Usuario"] as Usuario;
            List<UsuarioDocumento> documentos = _documentoService.ObtenerDocumentosPorUsuario(firmado, userSession);
            dataGridDocumentos.DataSource = documentos;
            dataGridDocumentos.DataBind();

            lblNoDocumentos.Visible = documentos.Count == 0;
        }

        protected void chkFirmado_CheckedChanged(object sender, EventArgs e)
        {
            CargarDocumentos(chkFirmado.Checked);
        }

        protected void dataGridDocumentos_RowCommand(object sender, GridViewCommandEventArgs e)
        {
            int documentoId = Convert.ToInt32(e.CommandArgument);

            if (e.CommandName == "Leer")
            {
                MostrarDocumento(documentoId);

                var documentosLeidos = Session["DocumentosLeidos"] as HashSet<int>;
                if (!documentosLeidos.Contains(documentoId))
                {
                    documentosLeidos.Add(documentoId);
                }
            }
            else if (e.CommandName == "Firmar")
            {
                var documentosLeidos = Session["DocumentosLeidos"] as HashSet<int>;
                if (documentosLeidos.Contains(documentoId))
                {
                    FirmarDocumento(documentoId);
                    CargarDocumentos(chkFirmado.Checked);

                    ScriptManager.RegisterStartupScript(this, GetType(), "FirmaExitosa", "Swal.fire('Confirmación', 'El documento ha sido firmado exitosamente.', 'success');", true);
                }
                else
                {
                    ScriptManager.RegisterStartupScript(this, GetType(), "LeerAntesDeFirmar", "Swal.fire('Advertencia', 'Debe leer el documento antes de firmarlo.', 'warning');", true);
                }
            }
        }

        private void FirmarDocumento(int documentoId)
        {
            var userSession = Session["Usuario"] as Usuario;
            _documentoService.FirmarDocumento(documentoId, userSession);
        }

        private void MostrarDocumento(int documentoId)
        {
            byte[] contenido = _documentoService.ObtenerContenidoPorId(documentoId);
            if (contenido != null)
            {
                litDocumentoContenido.Text = $"<embed src='data:application/pdf;base64,{Convert.ToBase64String(contenido)}' width='100%' height='600px' type='application/pdf' />";
                
                ScriptManager.RegisterStartupScript(this, GetType(), "ShowModalScript", "$('#leerDocumentoModal').modal('show');", true);
            }
        }

        protected void ddlLanguage_SelectedIndexChanged(object sender, EventArgs e)
        {
            string selectedLanguage = ddlLanguage.SelectedValue;
            Session["SelectedLanguage"] = selectedLanguage;
            Response.Redirect(Request.RawUrl);
        }
    }
}