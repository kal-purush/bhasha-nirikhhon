using Com.Bateeq.Service.Merchandiser.Lib.Helpers;
using Com.Bateeq.Service.Merchandiser.Lib.ViewModels;
using iTextSharp.text;
using iTextSharp.text.pdf;
using System;
using System.IO;
using System.Net;

namespace Com.Bateeq.Service.Merchandiser.Lib.PdfTemplates
{
    public class CostCalculationSpecolPdfTemplate
    {
        public MemoryStream GeneratePdfTemplate(CostCalculationRetailViewModel viewModel)
        {
            BaseFont bf = BaseFont.CreateFont(BaseFont.HELVETICA, BaseFont.CP1250, BaseFont.NOT_EMBEDDED);
            BaseFont bf_bold = BaseFont.CreateFont(BaseFont.HELVETICA_BOLD, BaseFont.CP1250, BaseFont.NOT_EMBEDDED);
            Font normal_font = FontFactory.GetFont(BaseFont.HELVETICA, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 7);
            Font bold_font = FontFactory.GetFont(BaseFont.HELVETICA_BOLD, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 7);
            Font bold_font_8 = FontFactory.GetFont(BaseFont.HELVETICA_BOLD, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 8);
            Font font_9 = FontFactory.GetFont(BaseFont.HELVETICA, BaseFont.CP1250, BaseFont.NOT_EMBEDDED, 9);
            DateTime now = DateTime.Now;

            Document document = new Document(PageSize.A4, 10, 10, 10, 10);
            MemoryStream stream = new MemoryStream();
            PdfWriter writer = PdfWriter.GetInstance(document, stream);
            writer.CloseStream = false;
            document.Open();
            PdfContentByte cb = writer.DirectContent;

            float margin = 10;
            float printedOnHeight = 10;
            float startY = 840 - margin;

            #region Header
            cb.BeginText();
            cb.SetFontAndSize(bf, 10);
            cb.ShowTextAligned(PdfContentByte.ALIGN_LEFT, "PT. EFRATA RETAILINDO", 10, 820, 0);
            cb.SetFontAndSize(bf_bold, 12);
            cb.ShowTextAligned(PdfContentByte.ALIGN_LEFT, "COST CALCULATION RETAIL", 10, 805, 0);
            cb.EndText();
            #endregion

            #region Top
            PdfPTable table_top = new PdfPTable(9);
            table_top.TotalWidth = 500f;

            float[] top_widths = new float[] { 1f, 0.1f, 2f, 1f, 0.1f, 2f, 1f, 0.1f, 2f };
            table_top.SetWidths(top_widths);

            PdfPCell cell_top = new PdfPCell() { Border = Rectangle.NO_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_MIDDLE, PaddingRight = 1, PaddingBottom = 2, PaddingTop = 2 };
            PdfPCell cell_colon = new PdfPCell() { Border = Rectangle.NO_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_MIDDLE };
            PdfPCell cell_top_keterangan = new PdfPCell() { Border = Rectangle.NO_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_MIDDLE, PaddingRight = 1, PaddingBottom = 2, PaddingTop = 2, Colspan = 7 };
            cell_colon.Phrase = new Phrase(":", normal_font);

            cell_top.Phrase = new Phrase("RO", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.RO}", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("BUYER", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.Buyer.Name}", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("OL", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            double OLValue = viewModel.OL.Value ?? 0;
            string OL = OLValue > 0 ? OLValue.ToString() + " menit" : OLValue.ToString();
            cell_top.Phrase = new Phrase($"{OL}", normal_font);
            table_top.AddCell(cell_top);

            cell_top.Phrase = new Phrase("ARTICLE", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.Article}", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("DELIVERY", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.DeliveryDate.ToString("dd MMMM yyyy")}", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("OTL 1", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            double OTL1Value = viewModel.OTL1.Value ?? 0;
            string OTL1 = OTL1Value > 0 ? OTL1Value.ToString() + " detik" : OTL1Value.ToString();
            cell_top.Phrase = new Phrase($"{OTL1}", normal_font);
            table_top.AddCell(cell_top);

            cell_top.Phrase = new Phrase("STYLE", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.Style.name}", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("SIZE RANGE", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.SizeRange.Name}", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("OTL 2", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            double OTL2Value = viewModel.OTL2.Value ?? 0;
            string OTL2 = OTL2Value > 0 ? OTL2Value.ToString() + " detik" : OTL2Value.ToString();
            cell_top.Phrase = new Phrase($"{OTL2}", normal_font);
            table_top.AddCell(cell_top);

            cell_top.Phrase = new Phrase("SEASON", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.Season.name}", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("EFFICIENCY", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.Efficiency.Value}%", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("OTL 3", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            double OTL3Value = viewModel.OTL3.Value ?? 0;
            string OTL3 = OTL3Value > 0 ? OTL3Value.ToString() + " detik" : OTL3Value.ToString();
            cell_top.Phrase = new Phrase($"{OTL3}", normal_font);
            table_top.AddCell(cell_top);

            cell_top.Phrase = new Phrase("COUNTER", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.Counter.name}", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("RISK", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top.Phrase = new Phrase($"{viewModel.Risk}%", normal_font);
            table_top.AddCell(cell_top);
            cell_top.Phrase = new Phrase("TOTAL SMV", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            double STD_HourValue = viewModel.SH_Cutting.Value + viewModel.SH_Finishing.Value + viewModel.SH_Sewing.Value;
            string STD_Hour = STD_HourValue > 0 ? STD_HourValue.ToString() : STD_HourValue.ToString();
            cell_top.Phrase = new Phrase($"{STD_Hour}", normal_font);
            table_top.AddCell(cell_top);

            cell_top.Phrase = new Phrase("KETERANGAN", normal_font);
            table_top.AddCell(cell_top);
            table_top.AddCell(cell_colon);
            cell_top_keterangan.Phrase = new Phrase($"{viewModel.Description}", normal_font);
            table_top.AddCell(cell_top_keterangan);
            #endregion

            #region Draw Image
            float imageHeight;
            try
            {
                byte[] imageByte = Convert.FromBase64String(Base64.GetBase64File(viewModel.ImageFile));
                Image image = Image.GetInstance(imgb: imageByte);
                if (image.Width > 60)
                {
                    float percentage = 0.0f;
                    percentage = 60 / image.Width;
                    image.ScalePercent(percentage * 100);
                }
                imageHeight = image.ScaledHeight;
                float imageY = 800 - imageHeight;
                image.SetAbsolutePosition(520, imageY);
                cb.AddImage(image, inlineImage: true);
            }
            catch (Exception)
            {
                imageHeight = 0;
            }
            #endregion

            #region Draw Top
            float row1Y = 800;
            table_top.WriteSelectedRows(0, -1, 10, row1Y, cb);
            #endregion

            #region Detail (Bottom, Column 1.2)
            PdfPTable table_detail = new PdfPTable(2);
            table_detail.TotalWidth = 280f;

            float[] detail_widths = new float[] { 1f, 1f };
            table_detail.SetWidths(detail_widths);

            PdfPCell cell_detail = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 5, Rowspan = 4 };

            double total = Convert.ToDouble(viewModel.OL.CalculatedValue + viewModel.OTL1.CalculatedValue + viewModel.OTL2.CalculatedValue + viewModel.OTL3.CalculatedValue);
            cell_detail.Phrase = new Phrase(
                "OL".PadRight(22) + ": " + viewModel.OL.CalculatedValue + Environment.NewLine + Environment.NewLine +
                "OTL 1".PadRight(20) + ": " + viewModel.OTL1.CalculatedValue + Environment.NewLine + Environment.NewLine +
                "OTL 2".PadRight(20) + ": " + viewModel.OTL2.CalculatedValue + Environment.NewLine + Environment.NewLine +
                "OTL 3".PadRight(20) + ": " + viewModel.OTL3.CalculatedValue + Environment.NewLine + Environment.NewLine +
                "Total".PadRight(22) + ": " + total + Environment.NewLine
                , normal_font);
            table_detail.AddCell(cell_detail);

            cell_detail = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_BOTTOM, Padding = 5 };
            cell_detail.Phrase = new Phrase("HPP", normal_font);
            table_detail.AddCell(cell_detail);
            cell_detail = new PdfPCell() { Border = Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_TOP, Padding = 5 };
            cell_detail.Phrase = new Phrase(Number.ToRupiah(viewModel.HPP), font_9);
            table_detail.AddCell(cell_detail);
            cell_detail = new PdfPCell() { Border = Rectangle.LEFT_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_BOTTOM, Padding = 5 };
            cell_detail.Phrase = new Phrase("Wholesale Price: HPP X 2.20", normal_font);
            table_detail.AddCell(cell_detail);
            cell_detail = new PdfPCell() { Border = Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_TOP, Padding = 5 };
            cell_detail.Phrase = new Phrase(Number.ToRupiah(viewModel.WholesalePrice), font_9);
            table_detail.AddCell(cell_detail);
            #endregion

            #region Signature (Bottom, Column 1.2)
            PdfPTable table_signature = new PdfPTable(3);
            table_signature.TotalWidth = 280f;

            float[] signature_widths = new float[] { 1f, 1f, 1f };
            table_signature.SetWidths(signature_widths);

            PdfPCell cell_signature = new PdfPCell() { Border = Rectangle.NO_BORDER, HorizontalAlignment = Element.ALIGN_CENTER, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2 };

            cell_signature.Phrase = new Phrase("Mengetahui,", normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase("", normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase("Menyetujui,", normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase("Ka. Sie Merchandiser", normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase("Creative Director", normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase("Komisaris", normal_font);
            table_signature.AddCell(cell_signature);

            string signatureArea = string.Empty;
            for (int i = 0; i < 5; i++)
            {
                signatureArea += Environment.NewLine;
            }
            cell_signature.Phrase = new Phrase(signatureArea, normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase(signatureArea, normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase(signatureArea, normal_font);
            table_signature.AddCell(cell_signature);

            cell_signature.Phrase = new Phrase("Dwi Fajar", normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase("Ari Seputra", normal_font);
            table_signature.AddCell(cell_signature);
            cell_signature.Phrase = new Phrase("Sistha Alicia Tjokrosaputro", normal_font);
            table_signature.AddCell(cell_signature);
            #endregion

            #region Price (Bottom, Column 2)
            PdfPTable table_price = new PdfPTable(5);
            table_price.TotalWidth = 280f;

            float[] price_widths = new float[] { 1.6f, 3f, 3f, 4f, 1f };
            table_price.SetWidths(price_widths);

            PdfPCell cell_price_center = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_CENTER, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2 };
            PdfPCell cell_price_left = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2 };
            PdfPCell cell_price_right = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_RIGHT, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2 };

            cell_price_center.Phrase = new Phrase("KET (X)", bold_font);
            table_price.AddCell(cell_price_center);
            cell_price_center.Phrase = new Phrase("HARGA (Rp)", bold_font);
            table_price.AddCell(cell_price_center);
            cell_price_center.Phrase = new Phrase("PEMBULATAN HARGA (Rp)", bold_font);
            table_price.AddCell(cell_price_center);
            cell_price_center.Phrase = new Phrase("KETERANGAN", bold_font);
            table_price.AddCell(cell_price_center);
            cell_price_center.Phrase = new Phrase("", bold_font);
            table_price.AddCell(cell_price_center);

            cell_price_left.Phrase = new Phrase("2", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed20), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding20), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding20") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.1", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed21), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding21), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding21") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.2", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed22), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding22), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding22") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.3", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed23), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding23), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding23") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.4", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed24), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding24), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding24") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.5", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed25), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding25), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding25") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.6", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed26), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding26), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding26") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.7", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed27), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding27), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding27") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.8", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed28), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding28), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding28") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("2.9", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed29), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding29), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding29") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("3.0", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Proposed30), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.Rounding30), normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("Rounding30") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            
            cell_price_left.Phrase = new Phrase("Others", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_right.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_right.Phrase = new Phrase(viewModel.RoundingOthers > 0 ? Number.ToRupiahWithoutSymbol(viewModel.RoundingOthers) : "", normal_font);
            table_price.AddCell(cell_price_right);
            cell_price_left.Phrase = new Phrase("", normal_font);
            table_price.AddCell(cell_price_left);
            cell_price_center.Phrase = new Phrase(viewModel.SelectedRounding.ToString().Equals("RoundingOthers") ? "*" : "", normal_font);
            table_price.AddCell(cell_price_center);
            #endregion

            #region Cost Calculation Material
            PdfPTable table_ccm = new PdfPTable(7);
            table_ccm.TotalWidth = 570f;

            float[] ccm_widths = new float[] { 1.25f, 3.5f, 4f, 9f, 3f, 4f, 4f };
            table_ccm.SetWidths(ccm_widths);

            PdfPCell cell_ccm_center = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_CENTER, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2 };
            PdfPCell cell_ccm_left = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_LEFT, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2 };
            PdfPCell cell_ccm_right = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_RIGHT, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2 };

            cell_ccm_center.Phrase = new Phrase("NO", bold_font);
            table_ccm.AddCell(cell_ccm_center);

            cell_ccm_center.Phrase = new Phrase("CATEGORIES", bold_font);
            table_ccm.AddCell(cell_ccm_center);

            cell_ccm_center.Phrase = new Phrase("MATERIALS", bold_font);
            table_ccm.AddCell(cell_ccm_center);

            cell_ccm_center.Phrase = new Phrase("DESCRIPTION", bold_font);
            table_ccm.AddCell(cell_ccm_center);

            cell_ccm_center.Phrase = new Phrase("QUANTITY", bold_font);
            table_ccm.AddCell(cell_ccm_center);

            cell_ccm_center.Phrase = new Phrase("RP. PTC/PC", bold_font);
            table_ccm.AddCell(cell_ccm_center);

            cell_ccm_center.Phrase = new Phrase("RP. TOTAL", bold_font);
            table_ccm.AddCell(cell_ccm_center);

            double Total = 0;
            float row1Height = imageHeight > table_top.TotalHeight ? imageHeight : table_top.TotalHeight;
            float row2Y = row1Y - row1Height - 10;
            float calculatedHppHeight = 7;
            float row3LeftHeight = table_detail.TotalHeight + 5 + table_signature.TotalHeight;
            float row3RightHeight = table_price.TotalHeight;
            float row3Height = row3LeftHeight > row3RightHeight ? row3LeftHeight : row3RightHeight;
            float remainingRow2Height = row2Y - 10 - row3Height - printedOnHeight - margin;
            float allowedRow2Height = row2Y - printedOnHeight - margin;
            for (int i = 0; i < viewModel.CostCalculationRetail_Materials.Count; i++)
            {
                cell_ccm_center.Phrase = new Phrase((i + 1).ToString(), normal_font);
                table_ccm.AddCell(cell_ccm_center);

                cell_ccm_left.Phrase = new Phrase(viewModel.CostCalculationRetail_Materials[i].Category.SubCategory != null ? String.Format("{0} - {1}", viewModel.CostCalculationRetail_Materials[i].Category.Name, viewModel.CostCalculationRetail_Materials[i].Category.SubCategory) : viewModel.CostCalculationRetail_Materials[i].Category.Name, normal_font);
                table_ccm.AddCell(cell_ccm_left);

                cell_ccm_left.Phrase = new Phrase(viewModel.CostCalculationRetail_Materials[i].Material.Name, normal_font);
                table_ccm.AddCell(cell_ccm_left);

                cell_ccm_left.Phrase = new Phrase(viewModel.CostCalculationRetail_Materials[i].Description, normal_font);
                table_ccm.AddCell(cell_ccm_left);

                cell_ccm_right.Phrase = new Phrase(String.Format("{0} {1}", viewModel.CostCalculationRetail_Materials[i].Quantity, viewModel.CostCalculationRetail_Materials[i].UOMQuantity.Name), normal_font);
                table_ccm.AddCell(cell_ccm_right);

                cell_ccm_right.Phrase = new Phrase(String.Format("{0}/{1}", Number.ToRupiahWithoutSymbol(viewModel.CostCalculationRetail_Materials[i].Price), viewModel.CostCalculationRetail_Materials[i].UOMPrice.Name), normal_font);
                table_ccm.AddCell(cell_ccm_right);

                cell_ccm_right.Phrase = new Phrase(Number.ToRupiahWithoutSymbol(viewModel.CostCalculationRetail_Materials[i].Total), normal_font);
                table_ccm.AddCell(cell_ccm_right);
                Total += viewModel.CostCalculationRetail_Materials[i].Total;

                float currentHeight = table_ccm.TotalHeight;
                if (currentHeight / remainingRow2Height > 1)
                {
                    if (currentHeight / allowedRow2Height > 1)
                    {
                        PdfPRow headerRow = table_ccm.GetRow(0);
                        PdfPRow lastRow = table_ccm.GetRow(table_ccm.Rows.Count - 1);
                        table_ccm.DeleteLastRow();
                        table_ccm.WriteSelectedRows(0, -1, 10, row2Y, cb);
                        table_ccm.DeleteBodyRows();
                        this.DrawPrintedOn(now, bf, cb);
                        document.NewPage();
                        table_ccm.Rows.Add(headerRow);
                        table_ccm.Rows.Add(lastRow);
                        table_ccm.CalculateHeights();
                        row2Y = startY;
                        remainingRow2Height = row2Y - 10 - row3Height - printedOnHeight - margin;
                        allowedRow2Height = row2Y - printedOnHeight - margin;
                    }
                }
            }

            cell_ccm_right = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_RIGHT, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2, Colspan = 6 };
            cell_ccm_right.Phrase = new Phrase("TOTAL", bold_font_8);
            table_ccm.AddCell(cell_ccm_right);
            cell_ccm_right = new PdfPCell() { Border = Rectangle.TOP_BORDER | Rectangle.LEFT_BORDER | Rectangle.BOTTOM_BORDER | Rectangle.RIGHT_BORDER, HorizontalAlignment = Element.ALIGN_RIGHT, VerticalAlignment = Element.ALIGN_MIDDLE, Padding = 2 };
            cell_ccm_right.Phrase = new Phrase(Number.ToRupiah(Total), bold_font_8);
            table_ccm.AddCell(cell_ccm_right);
            #endregion

            #region Draw Middle and Bottom
            table_ccm.WriteSelectedRows(0, -1, 10, row2Y, cb);

            float row3Y = row2Y - table_ccm.TotalHeight - 10;
            float remainingRow3Height = row3Y - printedOnHeight - margin;
            if (remainingRow3Height < row3Height)
            {
                this.DrawPrintedOn(now, bf, cb);
                row3Y = startY;
                document.NewPage();
            }

            #region Calculated HPP
            float calculatedHppY = row3Y - calculatedHppHeight;
            cb.BeginText();
            cb.SetFontAndSize(bf, 8);
            cb.ShowTextAligned(PdfContentByte.ALIGN_LEFT, "KALKULASI HPP: (OL + OTL1 + OTL2 + FABRIC + ACC) + ((OL + OTL1 + OTL2 + FABRIC + ACC) * Risk)", 10, calculatedHppY, 0);
            cb.EndText();
            #endregion

            float table_detailY = calculatedHppY - 5;
            table_detail.WriteSelectedRows(0, -1, 10, table_detailY, cb);

            float table_signatureY = table_detailY - row3Height + table_signature.TotalHeight;
            table_signature.WriteSelectedRows(0, -1, 10, table_signatureY, cb);

            table_price.WriteSelectedRows(0, -1, 300, table_detailY, cb);

            this.DrawPrintedOn(now, bf, cb);
            #endregion

            document.Close();

            byte[] byteInfo = stream.ToArray();
            stream.Write(byteInfo, 0, byteInfo.Length);
            stream.Position = 0;

            return stream;
        }

        private void DrawPrintedOn(DateTime now, BaseFont bf, PdfContentByte cb)
        {
            cb.BeginText();
            cb.SetFontAndSize(bf, 6);
            cb.ShowTextAligned(PdfContentByte.ALIGN_LEFT, "Printed on: " + now.ToString("dd/MM/yyyy | HH:mm"), 10, 10, 0);
            cb.EndText();
        }
    }
}