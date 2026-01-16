using Com.Danliris.Service.Sales.Lib.BusinessLogic.Interface.CostCalculationGarmentLogic;
using Com.Danliris.Service.Sales.Lib.Models.CostCalculationGarments;
using Com.Danliris.Service.Sales.Lib.ViewModels.CostCalculationGarment;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Logic.CostCalculationGarments;
using Com.Danliris.Service.Sales.Lib.Services;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Com.Danliris.Service.Sales.Lib.Helpers;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using System.Drawing;
using Com.Moonlay.NetCore.Lib;

namespace Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades.CostCalculationGarments
{
    public class CostCalculationGarmentValidationReportFacade : ICostCalculationGarmentValidationReport
    {
        private readonly SalesDbContext DbContext;
        private readonly DbSet<CostCalculationGarment> DbSet;
        private IdentityService IdentityService;
        private CostCalculationGarmentValidationReportLogic CostCalculationGarmentValidationReportLogic;

        public CostCalculationGarmentValidationReportFacade(IServiceProvider serviceProvider, SalesDbContext dbContext)
        {
            this.DbContext = dbContext;
            this.DbSet = this.DbContext.Set<CostCalculationGarment>();
            this.IdentityService = serviceProvider.GetService<IdentityService>();
            this.CostCalculationGarmentValidationReportLogic = serviceProvider.GetService<CostCalculationGarmentValidationReportLogic>();
        }
        
        public Tuple<MemoryStream, string> GenerateExcel(string filter = "{}")
        {

            Dictionary<string, string> FilterDictionary = new Dictionary<string, string>(JsonConvert.DeserializeObject<Dictionary<string, string>>(filter), StringComparer.OrdinalIgnoreCase);

            var Query = CostCalculationGarmentValidationReportLogic.GetQuery(filter);
            var data = Query.ToList();
            DataTable result = new DataTable();
            var offset = 7;
           
            result.Columns.Add(new DataColumn() { ColumnName = "No", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Nomor RO", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Nama Staff", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Nama Unit", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Tgl Confirm", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Seksi", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Kode Agent", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Nama Agent", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Kode Brand", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Nama Brand", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Komoditi", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Article", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Shipment", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Jumlah", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Satuan", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Valid Kabag Merchandiser", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Valid Bagian Engineering", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Valid Kabag Pucrhasing", DataType = typeof(String) });
            result.Columns.Add(new DataColumn() { ColumnName = "Valid Kadiv Merchandiser", DataType = typeof(String) });
     
            Dictionary<string, string> Rowcount = new Dictionary<string, string>();
            if (Query.ToArray().Count() == 0)
                     result.Rows.Add("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""); // to allow column name to be generated properly for empty data as template
            else
            {
                int index = 0;
                foreach (CostCalculationGarmentValidationReportViewModel item in data)
                {
                            index++;
                            string CfrmDate = item.ConfirmDate == new DateTime(1970, 1, 1) ? "-" : item.ConfirmDate.ToOffset(new TimeSpan(offset, 0, 0)).ToString("dd MMM yyyy", new CultureInfo("id-ID"));
                            string ShipDate = item.DeliveryDate == new DateTime(1970, 1, 1) ? "-" : item.DeliveryDate.ToOffset(new TimeSpan(offset, 0, 0)).ToString("dd MMM yyyy", new CultureInfo("id-ID"));
                            string QtyOrder = string.Format("{0:N2}", item.Quantity);
                    
                    result.Rows.Add(index, item.RO_Number, item.StaffName, item.UnitName, CfrmDate, item.Section, item.BuyerCode, item.BuyerName, item.BrandCode, item.BrandName,
                                    item.Comodity, item.Article, ShipDate, QtyOrder, item.UOMUnit, item.ValidatedMD, item.ValidatedIE, item.ValidatedPurch, item.ValidatedKadiv);
                }
            }
            ExcelPackage package = new ExcelPackage();
            var sheet = package.Workbook.Worksheets.Add("VALIDASI COST CALCULATION GARMENT");
            sheet.Cells["A1"].LoadFromDataTable(result, true, OfficeOpenXml.Table.TableStyles.Light16);

            sheet.Cells[sheet.Dimension.Address].AutoFitColumns();
            MemoryStream streamExcel = new MemoryStream();
            package.SaveAs(streamExcel);

            string fileName = string.Concat("Validasi CC Garment Per Unit", ".xlsx");

            return Tuple.Create(streamExcel, fileName);

        }


        public Tuple<List<CostCalculationGarmentValidationReportViewModel>, int> Read(int page = 1, int size = 25, string filter = "{}")
        {
            var Query = CostCalculationGarmentValidationReportLogic.GetQuery(filter);
            var data = Query.ToList();

            Pageable<CostCalculationGarmentValidationReportViewModel> pageable = new Pageable<CostCalculationGarmentValidationReportViewModel>(data, page - 1, size);
            List<CostCalculationGarmentValidationReportViewModel> Data_ = pageable.Data.ToList<CostCalculationGarmentValidationReportViewModel>();

            int TotalData = pageable.TotalCount;

            return Tuple.Create(Data_, TotalData);
        }     
    }
}