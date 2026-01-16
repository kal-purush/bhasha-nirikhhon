using OfficeOpenXml;
using OfficeOpenXml.Style;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.Services.OthersExpenditureProofDocument.ExcelGenerator
{
    public static class OthersExpenditureProofDocumentExcelGenerator
    {
        public static MemoryStream Create(string filename, List<OthersExpenditureProofDocumentReportViewModel> results, string expenditureNo, DateTimeOffset dateExpenditure, string division, DateTimeOffset startDate, DateTimeOffset endDate, int timezoneOffset)
        {
            var memoryStream = new MemoryStream();
            var startDateStr = startDate.Year == 1 ? "-" : startDate.ToString("dd-MM-yyyy");
            var endDateStr = endDate.Year == 1 ? "-" : endDate.ToString("dd-MM-yyyy");
            var dateExpenditureStr = dateExpenditure.Year == 1 ? "-" : dateExpenditure.ToString("dd-MM-yyyy");
            var countData = results.Count;
            var dataTable = new DataTable();

            using (var package = new ExcelPackage(memoryStream))
            {
                var firstelement = results.FirstOrDefault();
                var worksheet = package.Workbook.Worksheets.Add(filename);

                var listHeaderTable = new List<string> {
                    "No",
                    "Nomor",
                    "Tanggal",
                    "Nama Bank",
                    "Mata Uang",
                    "Total",
                    "Jenis Transaksi",
                    "No Cek/BG",
                    "Keterangan"
                };
                var listFilter = new List<string>
                {
                    $"Filter : ",
                    $"No Bukti Pengeluaran : {expenditureNo}",
                    $"Tanggal Pengeluaran : {dateExpenditureStr}",
                    $"Divisi : {division}",
                    $"Tanggal Awal Pengeluaran : {startDateStr}",
                    $"Tanggal Akhir Pengeluaran : {endDateStr}"
                };
                int row = 1, col = 1, countHeader = listHeaderTable.Count;

                worksheet.Cells[row, col].Value = "Laporan Bukti Pengeluaran Lain - Lain";
                worksheet.Cells[row, col].Style.Font.Bold = true;
                worksheet.Cells[row, col].Style.Font.UnderLineType = ExcelUnderLineType.Single;
                worksheet.Cells[row, col].Style.Font.UnderLine = true;
                worksheet.Cells[row, col, row, countHeader].Style.Font.Name = "Calibri";
                worksheet.Cells[row, col, row, countHeader].Style.Font.Size = 15f;
                worksheet.Cells[row, col, row, countHeader].Merge = true;
                row += 2;

                #region title header
                foreach (var filter in listFilter)
                {
                    worksheet.Cells[row, col].Value = filter;
                    worksheet.Cells[row, col, row, countHeader].Style.Font.Name = "Calibri";
                    worksheet.Cells[row, col, row, countHeader].Style.Font.Size = 12f;
                    worksheet.Cells[row, col, row, countHeader].Merge = true;
                    row++;
                }
                row += 3;
                #endregion

                #region Table

                #region HeaderTable
                for (int index = 0; index < listHeaderTable.Count; index++)
                {
                    var text = listHeaderTable[index];
                    if (index > 0 && index < 3)
                    {
                        worksheet.Cells[row, col].Value = "Bukti Pengeluaran Lain - Lain";
                        SetFormatHeaderTable(worksheet.Cells[row, col]);
                        worksheet.Cells[row, col, row, col + 1].Merge = true;

                        dataTable.Columns.Add(new DataColumn(listHeaderTable[index]));
                        worksheet.Cells[row + 1, col].Value = listHeaderTable[index];
                        SetFormatHeaderTable(worksheet.Cells[row + 1, col]);
                        index++;
                        col++;

                        dataTable.Columns.Add(new DataColumn(listHeaderTable[index]));
                        worksheet.Cells[row + 1, col].Value = listHeaderTable[index];
                        SetFormatHeaderTable(worksheet.Cells[row + 1, col]);
                        col++;
                    }
                    else
                    {
                        dataTable.Columns.Add(new DataColumn(text));
                        worksheet.Cells[row, col].Value = text;
                        SetFormatHeaderTable(worksheet.Cells[row, col]);
                        worksheet.Cells[row, col, row + 1, col].Merge = true;
                        col++;
                    }
                }
                col = 1;
                row += 2;
                #endregion

                #region BodyTable
                var dataWithIndex = results.Select((item, index) => new { item, Index = index + 1 });
                foreach (var data in dataWithIndex)
                {
                    dataTable.Rows.Add(data.Index, data.item.DocumentNo, data.item.Date, data.item.BankName, data.item.CurrencyCode, data.item.Total, data.item.Type, data.item.CekBgNo, data.item.Remark);
                }
                worksheet.Cells[row, col].LoadFromDataTable(dataTable, false);
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].AutoFitColumns();
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Top.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Left.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Right.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Bottom.Style = ExcelBorderStyle.Thin;
                #endregion
                row += countData;
                #region Total
                var total = results.Sum(s => s.Total);
                var totalStr = total.ToString("N2");
                worksheet.Cells[row, col].Value = "Total";
                worksheet.Cells[row, col, row, col + 4].Merge = true;
                worksheet.Cells[row, col, row, col + 4].Style.Border.Top.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row, col + 4].Style.Border.Left.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row, col + 4].Style.Border.Right.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row, col + 4].Style.Border.Bottom.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row, col + 4].Style.HorizontalAlignment = ExcelHorizontalAlignment.Right;
                worksheet.Cells[row, col, row, col + 4].Style.Font.Bold = true;

                worksheet.Cells[row, col].Value = totalStr;
                worksheet.Cells[row, col + 4, row, col + 9].Merge = true;
                worksheet.Cells[row, col + 4, row, col + 9].Style.Border.Top.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col + 4, row, col + 9].Style.Border.Left.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col + 4, row, col + 9].Style.Border.Right.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col + 4, row, col + 9].Style.Border.Bottom.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col + 4, row, col + 9].Style.HorizontalAlignment = ExcelHorizontalAlignment.Left;
                worksheet.Cells[row, col + 4, row, col + 9].Style.Font.Bold = true;
                #endregion

                #endregion
                MemoryStream stream = new MemoryStream();
                package.SaveAs(stream);
                return stream;
            }
        }
        public static void SetFormatHeaderTable(ExcelRange range)
        {
            range.Style.Border.BorderAround(ExcelBorderStyle.Thin);
            range.Style.Fill.PatternType = ExcelFillStyle.Solid;
            range.Style.Font.Bold = true;
            range.Style.Font.Name = "Calibri";
            range.Style.Font.Size = 12f;
            range.Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
            range.Style.HorizontalAlignment = ExcelHorizontalAlignment.CenterContinuous;
            range.Style.VerticalAlignment = ExcelVerticalAlignment.Center;
        }
        public static void SetFormatDataTable(ExcelRange range)
        {
            range.Style.Border.BorderAround(ExcelBorderStyle.Thin);
            range.Style.Font.Name = "Calibri";
            range.Style.Font.Size = 12f;
            range.Style.HorizontalAlignment = ExcelHorizontalAlignment.CenterContinuous;
            range.Style.VerticalAlignment = ExcelVerticalAlignment.Center;
        }


    }
}