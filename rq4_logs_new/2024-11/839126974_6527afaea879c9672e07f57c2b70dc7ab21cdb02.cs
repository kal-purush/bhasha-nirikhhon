using iTextSharp.text;
using iTextSharp.text.pdf;
using Models;
using System.Collections.Generic;
using System.IO;
using System.Web;

namespace Aplication.Services
{
    public class GenerarPDFService
    {
        public byte[] GenerarPdf(Models.Compra compra, Usuario usuario, List<DetalleCompra> detallesCompra)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            {
                Document document = new Document(PageSize.A4, 25, 25, 30, 50);
                PdfWriter writer = PdfWriter.GetInstance(document, memoryStream);
                document.Open();

                Font fontHeader = FontFactory.GetFont("Arial", 18, Font.BOLD, new BaseColor(106, 181, 71));
                Font fontText = FontFactory.GetFont("Arial", 12, BaseColor.BLACK);
                Font fontBold = FontFactory.GetFont("Arial", 12, Font.BOLD, BaseColor.BLACK);

                Paragraph title = new Paragraph("Resumen de Compra", fontHeader);
                title.Alignment = Element.ALIGN_CENTER;
                document.Add(title);

                document.Add(new Paragraph(" "));

                PdfPTable infoTable = new PdfPTable(2) { WidthPercentage = 100 };
                infoTable.SpacingAfter = 10f;
                infoTable.AddCell(new PdfPCell(new Phrase("ID COMPRA:", fontBold)) { Border = Rectangle.NO_BORDER });
                infoTable.AddCell(new PdfPCell(new Phrase(compra.Id.ToString(), fontText)) { Border = Rectangle.NO_BORDER });
                infoTable.AddCell(new PdfPCell(new Phrase("Nombre:", fontBold)) { Border = Rectangle.NO_BORDER });
                infoTable.AddCell(new PdfPCell(new Phrase($"{usuario.Nombre} {usuario.Apellido}", fontText)) { Border = Rectangle.NO_BORDER });
                infoTable.AddCell(new PdfPCell(new Phrase("Fecha de Pago:", fontBold)) { Border = Rectangle.NO_BORDER });
                infoTable.AddCell(new PdfPCell(new Phrase(compra.FechaPago.ToShortDateString(), fontText)) { Border = Rectangle.NO_BORDER });
                infoTable.AddCell(new PdfPCell(new Phrase("Total de la Compra:", fontBold)) { Border = Rectangle.NO_BORDER });
                infoTable.AddCell(new PdfPCell(new Phrase($"$ {compra.Total:N2}", fontText)) { Border = Rectangle.NO_BORDER });
                document.Add(infoTable);

                PdfPTable detailsTable = new PdfPTable(4) { WidthPercentage = 100 };
                detailsTable.SetWidths(new float[] { 3, 1, 1, 1 });
                detailsTable.SpacingBefore = 10f;

                AddTableHeader(detailsTable, "Producto", fontBold, new BaseColor(106, 181, 71));
                AddTableHeader(detailsTable, "Cantidad", fontBold, new BaseColor(106, 181, 71));
                AddTableHeader(detailsTable, "Precio Unitario", fontBold, new BaseColor(106, 181, 71));
                AddTableHeader(detailsTable, "Subtotal", fontBold, new BaseColor(106, 181, 71));

                foreach (var detalle in detallesCompra)
                {
                    detailsTable.AddCell(new PdfPCell(new Phrase(detalle.NombreProducto, fontText)));
                    detailsTable.AddCell(new PdfPCell(new Phrase(detalle.Cantidad.ToString(), fontText)));
                    detailsTable.AddCell(new PdfPCell(new Phrase($"$ {detalle.PrecioUnitario:N2}", fontText)));
                    detailsTable.AddCell(new PdfPCell(new Phrase($"$ {(detalle.Cantidad * detalle.PrecioUnitario):N2}", fontText)));
                }
                document.Add(detailsTable);

                document.Add(new Paragraph(" "));

                string logoPath = HttpContext.Current.Server.MapPath("~/Content/Imagenes/Logo_HrHub.jpg");
                if (File.Exists(logoPath))
                {
                    Image logo = Image.GetInstance(logoPath);
                    logo.ScaleToFit(100f, 50f);
                    logo.Alignment = Image.ALIGN_RIGHT;
                    logo.SetAbsolutePosition(document.PageSize.Width - document.RightMargin - logo.ScaledWidth, document.BottomMargin / 2);
                    document.Add(logo);
                }

                document.Close();
                return memoryStream.ToArray();
            }
        }

        private void AddTableHeader(PdfPTable table, string header, Font font, BaseColor backgroundColor)
        {
            PdfPCell cell = new PdfPCell(new Phrase(header, font));
            cell.BackgroundColor = backgroundColor;
            cell.HorizontalAlignment = Element.ALIGN_CENTER;
            cell.Padding = 5;
            table.AddCell(cell);
        }
    }
}