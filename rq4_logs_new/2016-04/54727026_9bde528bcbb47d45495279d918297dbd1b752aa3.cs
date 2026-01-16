using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Dominio.EntidadesNegocio;
using Dominio.InterfacesRepositorio;
using FabricaRepositorios;
using System.Collections.Generic;
using System.IO;

namespace AppWeb
{
    public partial class AltaAnuncio : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if(!IsPostBack)
            {
                IRepositorioHabitaciones repoHab = FabricaReposBienvenidosUY.CrearRepositorioHabitacion();
                this.lstHabitacion.DataSource = repoHab.FindHabitacion(((Persona)Session["usuario"]).Id);
                this.lstHabitacion.DataBind();
               ((List<Foto>)Session["fotosAnuncio"]).Clear();
                ((List<RangoFechas>)Session["rangoFechasAnuncio"]).Clear();
                ((List<Habitacion>)Session["habitacionesAnuncio"]).Clear();
                
            }
        }

        protected void btnAlta_Click(object sender, EventArgs e)
        {
            IRepositorioAnuncios repoAnun = FabricaReposBienvenidosUY.CrearRepositorioAnuncio();
            Anuncio unA = new Anuncio
            {
                Descripcion = txtDescripcion.Text,
                Nombre = txtNombre.Text,
                Fotos = ((List<Foto>)Session["fotosAnuncio"]),
                Habitaciones = ((List<Habitacion>)Session["habitacionesAnuncio"]),
                ListaRangos = ((List<RangoFechas>)Session["rangoFechasAnuncio"]),
                Id = -1

            };
            
           // unA.Add();
           // agregar en repositorio anuncio un metodo para agregar que controle si la persona tiene un rol huesped

            


    }

        protected void Button3_Click(object sender, EventArgs e)
        {
            if (fupFotos.HasFile)
            {
                string fileName = Path.GetFileName(fupFotos.PostedFile.FileName);
                fupFotos.PostedFile.SaveAs(Server.MapPath("~/Images/") + fileName);
                Foto unaF = new Foto
                { 
                    Ruta = Server.MapPath("~/Images/") + fileName
                };
                ((List<Foto>)Session["fotosAnuncio"]).Add(unaF);         
                }
            this.lstFotos.DataSource = ((List<Foto>)Session["fotosAnuncio"]);
            this.lstFotos.DataBind();
            }

        protected void Button1_Click(object sender, EventArgs e)
        {
            DateTime f_ini = Calendar1.SelectedDate;
            DateTime f_fin = Calendar2.SelectedDate;
            RangoFechas unRF = new RangoFechas { Fecha_ini = f_ini, Fecha_fin = f_fin };
            ((List<RangoFechas>)Session["rangoFechasAnuncio"]).Add(unRF);
            this.lstRangos.DataSource = ((List<RangoFechas>)Session["rangoFechasAnuncio"]);
            this.lstRangos.DataBind();

        }

        protected void Button2_Click(object sender, EventArgs e)
        {
            IRepositorioHabitaciones repHab = FabricaReposBienvenidosUY.CrearRepositorioHabitacion();
            int idHab = Convert.ToInt32(this.lstHabitacion.SelectedValue);
            Habitacion unaH = repHab.FindById(idHab);
            ((List<Habitacion>)Session["habitacionesAnuncio"]).Add(unaH);
            this.lstHabitacionesSeleccionadas.DataSource = ((List<Habitacion>)Session["habitacionesAnuncio"]);
            this.lstHabitacionesSeleccionadas.DataBind();
        }
    }
}