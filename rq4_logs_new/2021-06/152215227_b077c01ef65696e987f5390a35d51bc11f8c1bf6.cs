using Com.Danliris.Service.Finance.Accounting.Lib.Models.DailyBankTransaction;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.Services.DailyBankTransaction.Excel
{
    public static class AutoDailyBankTransactionExcelGenerator
    {
        public static MemoryStream CreateIn(string filename, List<DailyBankTransactionModel> results, string referenceNo, int accountBankId, string accountBankName, string division, DateTimeOffset? startDate, DateTimeOffset? endDate, int timezoneOffset)
        {
            var memoryStream = new MemoryStream();
            var startDateStr = startDate.HasValue ? "-" : startDate.GetValueOrDefault().ToString("dd-MM-yyyy");
            var endDateStr = endDate.HasValue ? "-" : endDate.GetValueOrDefault().ToString("dd-MM-yyyy");
            var countData = results.Count;
            var dataTable = new DataTable();

            using (var package = new ExcelPackage(memoryStream))
            {
                var firstelement = results.FirstOrDefault();
                var worksheet = package.Workbook.Worksheets.Add(filename);

                var listHeaderTable = new List<string> {
                    "No",
                    "No Referensi",
                    "Tanggal",
                    "Nama Bank",
                    "Mata Uang",
                    "Jenis Referensi",
                    "Jenis Sumber",
                    "Status",
                    "Nominal",
                    "Keterangan"
                };
                var listFilter = new List<string>
                {
                    $"Filter : ",
                    $"No Referensi : {referenceNo}",
                    $"Nama Bank : {accountBankName}",
                    $"Divisi : {division}",
                    $"Tanggal Awal Bank Harian : {startDateStr}",
                    $"Tanggal Akhir Bank harian Masuk : {endDateStr}"
                };
                int row = 1, col = 1, countHeader = listHeaderTable.Count;

                worksheet.Cells[row, col].Value = "Laporan Bank Harian Masuk";
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
                row += 2;
                #endregion

                #region Table

                #region HeaderTable
                for (int index = 0; index < listHeaderTable.Count; index++)
                {
                    var text = listHeaderTable[index];

                    dataTable.Columns.Add(new DataColumn(text));
                    worksheet.Cells[row, col].Value = text;
                    SetFormatHeaderTable(worksheet.Cells[row, col]);
                    worksheet.Cells[row, col].Merge = true;
                    col++;
                }
                col = 1;
                row += 2;
                #endregion

                #region BodyTable
                var dataWithIndex = results.Select((item, index) => new { item, Index = index + 1 });
                foreach (var data in dataWithIndex)
                {
                    dataTable.Rows.Add(data.Index, data.item.Code, data.item.Date.ToString("dd/MM/yyyy"), data.item.AccountBankAccountName + " " + data.item.AccountBankName+" "+ data.item.DestinationBankAccountNumber+" "+data.item.AccountBankCurrencyCode, data.item.AccountBankCurrencyCode, data.item.ReferenceType, data.item.SourceType, "IN", data.item.Nominal, data.item.Remark);
                }
                worksheet.Cells[row, col].LoadFromDataTable(dataTable, false);
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].AutoFitColumns();
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Top.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Left.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Right.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Bottom.Style = ExcelBorderStyle.Thin;
                #endregion

                #endregion
                MemoryStream stream = new MemoryStream();
                package.SaveAs(stream);
                return stream;
            }
        }

        public static MemoryStream CreateOut(string filename, List<DailyBankTransactionModel> results, string referenceNo, int accountBankId, string accountBankName, string division, DateTimeOffset? startDate, DateTimeOffset? endDate, int timezoneOffset)
        {
            var memoryStream = new MemoryStream();
            var startDateStr = startDate.HasValue ? "-" : startDate.GetValueOrDefault().ToString("dd-MM-yyyy");
            var endDateStr = endDate.HasValue ? "-" : endDate.GetValueOrDefault().ToString("dd-MM-yyyy");
            var countData = results.Count;
            var dataTable = new DataTable();

            using (var package = new ExcelPackage(memoryStream))
            {
                var firstelement = results.FirstOrDefault();
                var worksheet = package.Workbook.Worksheets.Add(filename);

                var listHeaderTable = new List<string> {
                    "No",
                    "No Referensi",
                    "Tanggal",
                    "Nama Bank",
                    "Mata Uang",
                    "Jenis Referensi",
                    "Jenis Sumber",
                    "Status",
                    "Nominal",
                    "Keterangan"
                };
                var listFilter = new List<string>
                {
                    $"Filter : ",
                    $"No Referensi : {referenceNo}",
                    $"Nama Bank : {accountBankName}",
                    $"Divisi : {division}",
                    $"Tanggal Awal Bank Harian : {startDateStr}",
                    $"Tanggal Akhir Bank harian Masuk : {endDateStr}"
                };
                int row = 1, col = 1, countHeader = listHeaderTable.Count;

                worksheet.Cells[row, col].Value = "Laporan Bank Harian Keluar";
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
                row += 2;
                #endregion

                #region Table

                #region HeaderTable
                for (int index = 0; index < listHeaderTable.Count; index++)
                {
                    var text = listHeaderTable[index];

                    dataTable.Columns.Add(new DataColumn(text));
                    worksheet.Cells[row, col].Value = text;
                    SetFormatHeaderTable(worksheet.Cells[row, col]);
                    worksheet.Cells[row, col].Merge = true;
                    col++;
                }
                col = 1;
                row += 2;
                #endregion

                #region BodyTable
                var dataWithIndex = results.Select((item, index) => new { item, Index = index + 1 });
                foreach (var data in dataWithIndex)
                {
                    dataTable.Rows.Add(data.Index, data.item.Code, data.item.Date.ToString("dd/MM/yyyy"), data.item.AccountBankAccountName + " " + data.item.AccountBankName + " " + data.item.DestinationBankAccountNumber + " " + data.item.AccountBankCurrencyCode, data.item.AccountBankCurrencyCode, data.item.ReferenceType, data.item.SourceType, "OUT", data.item.Nominal, data.item.Remark);
                }
                worksheet.Cells[row, col].LoadFromDataTable(dataTable, false);
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].AutoFitColumns();
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Top.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Left.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Right.Style = ExcelBorderStyle.Thin;
                worksheet.Cells[row, col, row + countData, listHeaderTable.Count].Style.Border.Bottom.Style = ExcelBorderStyle.Thin;
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