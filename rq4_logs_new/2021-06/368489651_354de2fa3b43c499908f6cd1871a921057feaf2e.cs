using ExpensesTracker.Models;
using iTextSharp.text;
using iTextSharp.text.pdf;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Windows.Forms;

namespace ExpensesTracker.Servises
{
    public class PdfGenerator
    {
        public void GeneratePdf(List<AccountOperation> filteredOperations)
        {
            var doc = new Document();
            PdfWriter.GetInstance(doc, new FileStream("pdfTables.pdf", FileMode.Create));
            doc.Open();
            var table = new PdfPTable(4);

            table.AddCell("Date");
            table.AddCell("Category");
            table.AddCell("Amount");
            table.AddCell("Comment");

            foreach (var operation in filteredOperations)
            {
                table.AddCell(new Phrase(operation.Date.ToString(CultureInfo.CurrentCulture)));
                table.AddCell(operation.Category == null ? new Phrase("") : new Phrase(operation.Category));
                table.AddCell(new Phrase(operation.Amount.ToString(CultureInfo.CurrentCulture)));
                table.AddCell(new Phrase(operation.Comment));
            }

            doc.Add(table);
            doc.Close();
            MessageBox.Show("Pdf seved");
        }
    }
}