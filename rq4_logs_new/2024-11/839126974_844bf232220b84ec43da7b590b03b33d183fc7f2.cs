using Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace Aplication.Services.XML
{
    public class GenerarXML
    {
        public void GenerarXMLProductosPorMesYAnio(List<ProductoReporte> productos)
        {
            string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "App_Data", "ReporteProductosMasComprados.xml");
            Directory.CreateDirectory(Path.GetDirectoryName(path));

            using (XmlWriter writer = XmlWriter.Create(path, new XmlWriterSettings { Indent = true, Encoding = Encoding.UTF8 }))
            {
                writer.WriteStartDocument();
                writer.WriteStartElement("ReporteProductosMasComprados");

                var productosAgrupados = productos
                    .GroupBy(p => new { p.Anio, p.Mes })
                    .OrderByDescending(g => g.Key.Anio)
                    .ThenByDescending(g => g.Key.Mes);

                foreach (var grupo in productosAgrupados)
                {
                    writer.WriteStartElement("Anio");
                    writer.WriteAttributeString("valor", grupo.Key.Anio.ToString());

                    writer.WriteStartElement("Mes");
                    writer.WriteAttributeString("nombre", grupo.Key.Mes);

                    foreach (var producto in grupo)
                    {
                        writer.WriteStartElement("Producto");
                        writer.WriteElementString("Nombre", producto.Nombre);
                        writer.WriteElementString("VecesComprado", producto.VecesComprado.ToString());
                        writer.WriteEndElement();
                    }

                    writer.WriteEndElement();
                    writer.WriteEndElement();
                }

                writer.WriteEndElement();
                writer.WriteEndDocument();
            }
        }
    }
}