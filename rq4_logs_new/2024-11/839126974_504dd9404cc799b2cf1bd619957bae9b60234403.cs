using Aplication.Interfaces;
using Aplication.Services.XML;
using System;
using System.Web.Script.Serialization;
using System.Web.UI;
using Unity;

namespace GUI
{
    public partial class DocumentoReporte : System.Web.UI.Page
    {
        private IDocumentoService _documentoService;

        public DocumentoReporte()
        {
            _documentoService = Global.Container.Resolve<IDocumentoService>();
        }

        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected void btnGenerarReporte_Click(object sender, EventArgs e)
        {
            WebService.DocumentosService servicio = new WebService.DocumentosService();
            var datos = servicio.ObtenerPorcentajeFirmasPorDocumento();

            if (datos != null && datos.Count > 0)
            {
                JavaScriptSerializer jsSerializer = new JavaScriptSerializer();
                string jsonData = jsSerializer.Serialize(datos);

                string script = $"renderTortaGrafico({jsonData});";
                ScriptManager.RegisterStartupScript(this, this.GetType(), "renderGrafico", script, true);
            }
            else
            {
                string noDataScript = "Swal.fire('Sin datos', 'No hay datos disponibles para generar el reporte.', 'warning');";
                ScriptManager.RegisterStartupScript(this, this.GetType(), "sinDatosReporte", noDataScript, true);
            }
        }

        protected void btnGenerarXML_Click(object sender, EventArgs e)
        {
            WebService.DocumentosService servicio = new WebService.DocumentosService();
            var datos = servicio.ObtenerPorcentajeFirmasPorDocumento();

            if (datos != null && datos.Count > 0)
            {
                GenerarXML generadorXml = new GenerarXML();
                generadorXml.GenerarXMLFirmasDocumentos(datos);

                string successScript = "Swal.fire('XML Generado', 'El archivo XML ha sido generado y guardado correctamente.', 'success');";
                ScriptManager.RegisterStartupScript(this, this.GetType(), "xmlGenerado", successScript, true);
            }
            else
            {
                string noDataScript = "Swal.fire('Sin datos', 'No hay datos disponibles para generar el XML.', 'warning');";
                ScriptManager.RegisterStartupScript(this, this.GetType(), "sinDatosXML", noDataScript, true);
            }
        }
    }
}