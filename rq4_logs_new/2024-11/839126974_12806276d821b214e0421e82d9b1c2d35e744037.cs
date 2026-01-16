using System;
using System.Collections.Generic;
using System.Web.UI;
using System.Web.UI.WebControls;
using Aplication.Interfaces;
using Models;
using Unity;

namespace GUI
{
    public partial class MisCompras : Page
    {
        private readonly ICompraService _compraService;
        private readonly IUsuarioService _usuarioService;

        public MisCompras()
        {
            _compraService = Global.Container.Resolve<ICompraService>();
            _usuarioService = Global.Container.Resolve<IUsuarioService>();
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                CargarCompras();
            }
        }

        private void CargarCompras()
        {
            var userSession = Session["Usuario"] as Usuario;

            if (userSession == null)
            {
                Response.Redirect("Login.aspx");
                return;
            }

            List<Models.Compra> listadoCompras = _compraService.ObtenerComprasPorUsuario(userSession.Id);
            gvCompras.DataSource = listadoCompras;
            gvCompras.DataBind();
        }

        protected void gvCompras_RowCommand(object sender, GridViewCommandEventArgs e)
        {
            if (e.CommandName == "VerResumen")
            {
                int idCompra = int.Parse(e.CommandArgument.ToString());
                MostrarResumenCompra(idCompra);
            }
        }

        private void MostrarResumenCompra(int idCompra)
        {
            var compra = _compraService.ObtenerCompra(idCompra);
            var usuario = _usuarioService.ObtenerUsuarioPorId(compra.IdUsuario);
            var detallesCompra = _compraService.ObtenerDetalleCompra(idCompra);

            lblIdCompra.Text = compra.Id.ToString();
            lblFechaCompra.Text = compra.FechaPago.ToString("dd/MM/yyyy");
            lblTotalCompra.Text = compra.Total.ToString("C");

            gvDetallesCompra.DataSource = detallesCompra;
            gvDetallesCompra.DataBind();

            ScriptManager.RegisterStartupScript(this, GetType(), "ShowModalScript", "showResumenCompraModal();", true);
        }
    }
}