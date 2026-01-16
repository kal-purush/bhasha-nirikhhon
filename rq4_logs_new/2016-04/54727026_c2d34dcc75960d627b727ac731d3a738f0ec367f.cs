using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Dominio.EntidadesNegocio;
using Dominio.InterfacesRepositorio;
using FabricaRepositorios;

namespace AppWeb
{
    public partial class Pruebas : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected void btnListar_Click(object sender, EventArgs e)
        {
            IRepositorioUbicaciones repoUbi = FabricaReposBienvenidosUY.CrearRepositorioUbicacion();
            List<Ubicacion> ubicaciones = new List<Ubicacion>();
            ubicaciones = repoUbi.FindAll();
            if (ubicaciones != null)
            {
                this.lstUbi.DataSource = ubicaciones;
                this.lstUbi.DataBind();
            }
            else
                Label1.Text = "No hay organizaciones para mostrar.";

            IRepositorioRangoPrecios repoRango = FabricaReposBienvenidosUY.CrearRepositorioRangoPrecio();
            List<RangoPrecio> precios = new List<RangoPrecio>();
            precios = repoRango.FindAll();
            if (precios != null)
            {
                this.lbxRangos.DataSource = precios;
                this.lbxRangos.DataBind();
            }
            else
                Label1.Text = "No hay organizaciones para mostrar.";

            IRepositorioAlojamientos repoAlos = FabricaReposBienvenidosUY.CrearRepositorioAlojamiento();
            List<Alojamiento> alojamientos = new List<Alojamiento>();
            alojamientos = repoAlos.FindAll();
            if (alojamientos != null)
            {
                this.lbxAlojamientos.DataSource = alojamientos;
                this.lbxAlojamientos.DataBind();
            }
            else
                Label1.Text = "No hay organizaciones para mostrar.";

        }
    }
}