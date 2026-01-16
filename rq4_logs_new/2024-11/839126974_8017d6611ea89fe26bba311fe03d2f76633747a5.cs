using Aplication.Interfaces;
using Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Unity;

namespace GUI
{
    public partial class GestionPermisosUsuario : System.Web.UI.Page
    {
        private readonly IUsuarioService _usuarioService;
        private readonly IPermisoService _permisoService;

        public GestionPermisosUsuario()
        {
            _usuarioService = Global.Container.Resolve<IUsuarioService>();
            _permisoService = Global.Container.Resolve<IPermisoService>();
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                CargarUsuarios();
            }
        }

        private void CargarUsuarios()
        {
            drpUsuarios.DataSource = _usuarioService.ListarUsuarios();
            drpUsuarios.DataTextField = "Email";
            drpUsuarios.DataValueField = "Id";
            drpUsuarios.DataBind();
            drpUsuarios.Items.Insert(0, new ListItem("Seleccione un usuario", "0"));
        }

        protected void drpUsuarios_SelectedIndexChanged(object sender, EventArgs e)
        {
            int idUsuario = int.Parse(drpUsuarios.SelectedValue);
            if (idUsuario == 0)
            {
                lblNoPermisosNoAsignados.Visible = true;
                lblNoPermisosAsignados.Visible = true;
                gvPermisosNoAsignados.Visible = false;
                gvPermisosAsignados.Visible = false;
            }
            else
            {
                CargarPermisosNoAsignados(idUsuario);
                CargarPermisosAsignados(idUsuario);
            }
        }

        private void CargarPermisosNoAsignados(int idUsuario)
        {
            var permisosNoAsignados = _permisoService.ObtenerPermisosNoAsignadosPorUsuario(idUsuario);
            gvPermisosNoAsignados.DataSource = permisosNoAsignados;
            gvPermisosNoAsignados.DataBind();

            lblNoPermisosNoAsignados.Visible = permisosNoAsignados.Count == 0;
            gvPermisosNoAsignados.Visible = permisosNoAsignados.Count > 0;
        }

        private void CargarPermisosAsignados(int idUsuario)
        {
            var permisosAsignados = _permisoService.ObtenerPermisosAsignadosPorUsuario(idUsuario);
            gvPermisosAsignados.DataSource = permisosAsignados;
            gvPermisosAsignados.DataBind();

            lblNoPermisosAsignados.Visible = permisosAsignados.Count == 0;
            gvPermisosAsignados.Visible = permisosAsignados.Count > 0;
        }

        protected void btnAsignarPermisos_Click(object sender, EventArgs e)
        {
            try
            {
                var userSession = Session["Usuario"] as Usuario;
                int idUsuario = int.Parse(drpUsuarios.SelectedValue);

                foreach (GridViewRow row in gvPermisosNoAsignados.Rows)
                {
                    CheckBox chkSelect = (CheckBox)row.FindControl("chkSelect");
                    if (chkSelect != null && chkSelect.Checked)
                    {
                        int idPermiso = Convert.ToInt32(gvPermisosNoAsignados.DataKeys[row.RowIndex].Value);
                        _permisoService.AsignarPermisoAUsuario(idUsuario, idPermiso, userSession);
                    }
                }

                CargarPermisosNoAsignados(idUsuario);
                CargarPermisosAsignados(idUsuario);
            }
            catch (Exception ex)
            {
                throw new Exception("Error al asignar permisos: " + ex.Message);
            }
        }

        protected void btnQuitarPermisos_Click(object sender, EventArgs e)
        {
            try
            {
                var userSession = Session["Usuario"] as Usuario;
                int idUsuario = int.Parse(drpUsuarios.SelectedValue);

                foreach (GridViewRow row in gvPermisosAsignados.Rows)
                {
                    CheckBox chkSelect = (CheckBox)row.FindControl("chkSelect");
                    if (chkSelect != null && chkSelect.Checked)
                    {
                        int idPermiso = Convert.ToInt32(gvPermisosAsignados.DataKeys[row.RowIndex].Value);
                        _permisoService.QuitarPermisoAUsuario(idUsuario, idPermiso, userSession);
                    }
                }

                CargarPermisosNoAsignados(idUsuario);
                CargarPermisosAsignados(idUsuario);
            }
            catch (Exception ex)
            {
                throw new Exception("Error al quitar permisos: " + ex.Message);
            }
        }
    }
}