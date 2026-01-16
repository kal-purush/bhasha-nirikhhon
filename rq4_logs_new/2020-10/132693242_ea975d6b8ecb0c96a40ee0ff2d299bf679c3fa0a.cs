using Com.DanLiris.Service.Purchasing.Lib.ViewModels.UnitReceiptNote;
using iTextSharp.text;
using iTextSharp.text.pdf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Com.DanLiris.Service.Purchasing.Lib.PDFTemplates
{
    public static class ImportPurchasingBookReportPdfTemplate
    {
        private static readonly Font _headerFont = FontFactory.GetFont(BaseFont.HELVETICA_BOLD, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 18);
        private static readonly Font _subHeaderFont = FontFactory.GetFont(BaseFont.HELVETICA_BOLD, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 16);
        private static readonly Font _normalFont = FontFactory.GetFont(BaseFont.HELVETICA, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 9);
        private static readonly Font _smallFont = FontFactory.GetFont(BaseFont.HELVETICA, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 8);
        private static readonly Font _smallerFont = FontFactory.GetFont(BaseFont.HELVETICA, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 7);
        private static readonly Font _normalBoldFont = FontFactory.GetFont(BaseFont.HELVETICA_BOLD, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 9);
        private static readonly Font _smallBoldFont = FontFactory.GetFont(BaseFont.HELVETICA_BOLD, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 8);
        private static readonly Font _smallerBoldFont = FontFactory.GetFont(BaseFont.HELVETICA_BOLD, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 7);

        public static MemoryStream Generate(LocalPurchasingBookReportViewModel viewModel, int timezoneOffset, DateTime? dateFrom, DateTime? dateTo)
        {
            var d1 = dateFrom.GetValueOrDefault().ToUniversalTime();
            var d2 = (dateTo.HasValue ? dateTo.Value : DateTime.Now).ToUniversalTime();

            var document = new Document(PageSize.A4.Rotate(), 5, 5, 25, 25);
            var stream = new MemoryStream();
            PdfWriter.GetInstance(document, stream);
            document.Open();

            SetHeader(document, d1, d2);

            SetReportTable(document, viewModel.Reports, viewModel.GrandTotal, timezoneOffset);

            document.Add(new Paragraph("\n"));

            SetCategoryCurrencySummaryTable(document, viewModel.CategorySummaries, viewModel.CategorySummaryTotal, viewModel.CurrencySummaries);

            //SetCategoryTable(document, viewModel.CategorySummaries, viewModel.CategorySummaryTotal);

            //SetCurrencyTable(document, viewModel.CurrencySummaries);

            SetFooter(document);

            document.Close();
            byte[] byteInfo = stream.ToArray();
            stream.Write(byteInfo, 0, byteInfo.Length);
            stream.Position = 0;

            return stream;
        }

        private static void SetCategoryCurrencySummaryTable(Document document, List<Summary> categorySummaries, decimal categorySummaryTotal, List<Summary> currencySummaries)
        {
            var table = new PdfPTable(3)
            {
                WidthPercentage = 95,

            };

            var widths = new List<float>() { 6f, 1f, 3f };
            table.SetWidths(widths.ToArray());

            table.AddCell(GetCategorySummaryTable(categorySummaries, categorySummaryTotal));
            table.AddCell(new PdfPCell() { Border = Rectangle.NO_BORDER });
            table.AddCell(GetCurrencySummaryTable(currencySummaries));

            document.Add(table);
        }

        private static PdfPCell GetCurrencySummaryTable(List<Summary> currencySummaries)
        {
            var table = new PdfPTable(2)
            {
                WidthPercentage = 100
            };

            var widths = new List<float>() { 1f, 2f };
            table.SetWidths(widths.ToArray());

            // set header
            var cell = new PdfPCell()
            {
                HorizontalAlignment = Element.ALIGN_CENTER,
                VerticalAlignment = Element.ALIGN_CENTER
            };

            cell.Phrase = new Phrase("Mata Uang", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("Total", _smallerFont);
            table.AddCell(cell);

            foreach (var currency in currencySummaries)
            {
                cell.HorizontalAlignment = Element.ALIGN_CENTER;
                cell.Phrase = new Phrase(currency.CurrencyCode, _smallerFont);
                table.AddCell(cell);

                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                cell.Phrase = new Phrase(string.Format("{0:n}", currency.SubTotal), _smallerFont);
                table.AddCell(cell);
            }

            return new PdfPCell(table) { Border = Rectangle.NO_BORDER };
        }

        private static PdfPCell GetCategorySummaryTable(List<Summary> categorySummaries, decimal categorySummaryTotal)
        {
            var table = new PdfPTable(2)
            {
                WidthPercentage = 100
            };

            var widths = new List<float>() { 1f, 2f };
            table.SetWidths(widths.ToArray());

            // set header
            var cell = new PdfPCell()
            {
                HorizontalAlignment = Element.ALIGN_CENTER,
                VerticalAlignment = Element.ALIGN_CENTER
            };

            cell.Phrase = new Phrase("Kategori", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("Total (IDR)", _smallerFont);
            table.AddCell(cell);

            foreach (var categorySummary in categorySummaries)
            {
                cell.HorizontalAlignment = Element.ALIGN_CENTER;
                cell.Phrase = new Phrase(categorySummary.Category, _smallerFont);
                table.AddCell(cell);

                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                cell.Phrase = new Phrase(string.Format("{0:n}", categorySummary.SubTotal), _smallerFont);
                table.AddCell(cell);
            }

            cell.Phrase = new Phrase("", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase(string.Format("{0:n}", categorySummaryTotal), _smallerFont);
            table.AddCell(cell);

            return new PdfPCell(table) { Border = Rectangle.NO_BORDER };
        }

        private static void SetFooter(Document document)
        {

        }

        //private static void SetCurrencyTable(Document document, List<Summary> currencySummaries)
        //{

        //}

        //private static void SetCategoryTable(Document document, List<Summary> categorySummaries, decimal categorySummaryTotal)
        //{

        //}

        private static void SetReportTable(Document document, List<PurchasingReport> reports, decimal grandTotal, int timezoneOffset)
        {
            var table = new PdfPTable(21)
            {
                WidthPercentage = 95
            };

            var widths = new List<float>();
            for (var i = 0; i < 21; i++)
            {
                if (i == 10)
                {
                    widths.Add(1f);
                    continue;
                }

                if (i == 1)
                {
                    widths.Add(3f);
                    continue;
                }

                widths.Add(2f);
            }
            table.SetWidths(widths.ToArray());

            SetReportTableHeader(table);

            foreach (var element in reports)
            {
                var cell = new PdfPCell()
                {
                    HorizontalAlignment = Element.ALIGN_CENTER,
                    VerticalAlignment = Element.ALIGN_CENTER
                };

                var cellAlignRight = new PdfPCell()
                {
                    HorizontalAlignment = Element.ALIGN_RIGHT,
                    VerticalAlignment = Element.ALIGN_CENTER
                };

                cell.Phrase = new Phrase(element.ReceiptDate.AddHours(timezoneOffset).ToString("yyyy-dd-MM"), _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.SupplierName, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.ProductName, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.IPONo, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.DONo, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.URNNo, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.InvoiceNo, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.VATNo, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.UPONo, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.CategoryCode + " - " + element.CategoryName, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.UnitCode, _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase("", _smallerFont);
                table.AddCell(cell);

                cell.Phrase = new Phrase(element.PIBNo, _smallerFont);
                table.AddCell(cell);

                cellAlignRight.Phrase = new Phrase(string.Format("{0:n}", element.PIBBM), _smallerFont);
                table.AddCell(cellAlignRight);

                cellAlignRight.Phrase = new Phrase(string.Format("{0:n}", 0), _smallerFont);
                table.AddCell(cellAlignRight);

                cellAlignRight.Phrase = new Phrase(string.Format("{0:n}", 0), _smallerFont);
                table.AddCell(cellAlignRight);

                cellAlignRight.Phrase = new Phrase(string.Format("{0:n}", element.DPP), _smallerFont);
                table.AddCell(cellAlignRight);

                cellAlignRight.Phrase = new Phrase(string.Format("{0:n}", element.DPPCurrency), _smallerFont);
                table.AddCell(cellAlignRight);

                cellAlignRight.Phrase = new Phrase(string.Format("{0:n}", element.VAT), _smallerFont);
                table.AddCell(cellAlignRight);

                cellAlignRight.Phrase = new Phrase(string.Format("{0:n}", element.CurrencyRate), _smallerFont);
                table.AddCell(cellAlignRight);

                cellAlignRight.Phrase = new Phrase(string.Format("{0:n}", element.Total), _smallerFont);
                table.AddCell(cellAlignRight);
            }

            var cellGrandTotal = new PdfPCell()
            {
                HorizontalAlignment = Element.ALIGN_RIGHT,
                VerticalAlignment = Element.ALIGN_CENTER,
                Colspan = 20
            };

            cellGrandTotal.Phrase = new Phrase("Grand Total", _smallerBoldFont);
            table.AddCell(cellGrandTotal);

            cellGrandTotal.Phrase = new Phrase(string.Format("{0:n}", grandTotal), _smallerBoldFont);
            table.AddCell(cellGrandTotal);

            document.Add(table);
        }

        private static void SetReportTableHeader(PdfPTable table)
        {
            var cell = new PdfPCell()
            {
                HorizontalAlignment = Element.ALIGN_CENTER,
                VerticalAlignment = Element.ALIGN_CENTER
            };

            var cellColspan3 = new PdfPCell()
            {
                HorizontalAlignment = Element.ALIGN_CENTER,
                VerticalAlignment = Element.ALIGN_CENTER,
                Colspan = 3
            };

            var cellColspan5 = new PdfPCell()
            {
                HorizontalAlignment = Element.ALIGN_CENTER,
                VerticalAlignment = Element.ALIGN_CENTER,
                Colspan = 5
            };

            var cellRowspan2 = new PdfPCell()
            {
                HorizontalAlignment = Element.ALIGN_CENTER,
                VerticalAlignment = Element.ALIGN_CENTER,
                Rowspan = 2
            };

            cellRowspan2.Phrase = new Phrase("Tanggal", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("Supplier", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("Keterangan", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("No PO", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("No Surat Jalan", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("No Bon Penerimaan", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("No Invoice", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("No Faktur Pajak", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("No SPB/NI", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("Kategori", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("Unit", _smallerFont);
            table.AddCell(cellRowspan2);

            cellColspan5.Phrase = new Phrase("PIB", _smallerFont);
            table.AddCell(cellColspan5);

            cellColspan3.Phrase = new Phrase("Pembelian", _smallerFont);
            table.AddCell(cellColspan3);

            cellRowspan2.Phrase = new Phrase("Rate", _smallerFont);
            table.AddCell(cellRowspan2);

            cellRowspan2.Phrase = new Phrase("Total (IDR)", _smallerFont);
            table.AddCell(cellRowspan2);

            cell.Phrase = new Phrase("Tanggal", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("No", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("BM", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("PPh 22", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("PPN Impor", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("DPP", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("DPP Valas", _smallerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("PPN", _smallerFont);
            table.AddCell(cell);
        }

        private static void SetHeader(Document document, DateTime dateFrom, DateTime dateTo)
        {
            var table = new PdfPTable(1);
            var cell = new PdfPCell()
            {
                Border = Rectangle.NO_BORDER,
                HorizontalAlignment = Element.ALIGN_CENTER,
                Phrase = new Phrase("LAPORAN BUKU PEMBELIAN IMPORT", _headerFont)
            };
            table.AddCell(cell);

            cell.Phrase = new Phrase("", _headerFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase($"Periode {dateFrom:yyyy-dd-MM} s/d {dateTo:yyyy-dd-MM}", _subHeaderFont);
            table.AddCell(cell);

            cell.Phrase = new Phrase("", _headerFont);
            table.AddCell(cell);

            document.Add(table);
        }

    }
}