using Com.DanLiris.Service.Purchasing.Lib.Helpers;
using Com.DanLiris.Service.Purchasing.Lib.Helpers.ReadResponse;
using Com.DanLiris.Service.Purchasing.Lib.Models.GarmentStockOpnameModel;
using Com.DanLiris.Service.Purchasing.Lib.Models.GarmentUnitReceiptNoteModel;
using Com.DanLiris.Service.Purchasing.Lib.Services;
using Com.Moonlay.Models;
using Com.Moonlay.NetCore.Lib;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.DanLiris.Service.Purchasing.Lib.Facades.GarmentStockOpnameFacades
{
    public class GarmentStockOpnameFacade : IGarmentStockOpnameFacade
    {
        private string USER_AGENT = "GarmentStockOpnameFacade";

        private readonly IServiceProvider serviceProvider;
        private readonly IdentityService identityService;

        private readonly PurchasingDbContext dbContext;
        private readonly DbSet<GarmentStockOpname> dbSet;
        private readonly DbSet<GarmentDOItems> dbSetDOItem;

        public GarmentStockOpnameFacade(IServiceProvider serviceProvider, PurchasingDbContext dbContext)
        {
            this.serviceProvider = serviceProvider;
            identityService = (IdentityService)serviceProvider.GetService(typeof(IdentityService));

            this.dbContext = dbContext;
            dbSet = dbContext.Set<GarmentStockOpname>();
            dbSetDOItem = dbContext.Set<GarmentDOItems>();
        }

        public ReadResponse<object> Read(int Page = 1, int Size = 25, string Order = "{}", string Keyword = null, string Filter = "{}")
        {
            IQueryable<GarmentStockOpname> Query = dbSet;

            if (!string.IsNullOrWhiteSpace(identityService.Username))
            {
                Query = Query.Where(x => x.CreatedBy == identityService.Username);
            }

            Query = Query.Select(m => new GarmentStockOpname
            {
                Id = m.Id,
                LastModifiedUtc = m.LastModifiedUtc,

                Date = m.Date,
                UnitCode = m.UnitCode,
                UnitName = m.UnitName,
                StorageName = m.StorageName
            });

            List<string> searchAttributes = new List<string>()
            {
                "UnitCode", "UnitName", "StorageName"
            };

            Query = QueryHelper<GarmentStockOpname>.ConfigureSearch(Query, searchAttributes, Keyword);

            Dictionary<string, string> FilterDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(Filter);
            Query = QueryHelper<GarmentStockOpname>.ConfigureFilter(Query, FilterDictionary);

            Dictionary<string, string> OrderDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(Order);
            Query = QueryHelper<GarmentStockOpname>.ConfigureOrder(Query, OrderDictionary);

            Pageable<GarmentStockOpname> pageable = new Pageable<GarmentStockOpname>(Query, Page - 1, Size);
            int TotalData = pageable.TotalCount;

            List<object> ListData = new List<object>();
            ListData.AddRange(pageable.Data.Select(s => new
            {
                s.Id,
                s.LastModifiedUtc,

                s.Date,
                s.UnitCode,
                s.UnitName,
                s.StorageName
            }));

            return new ReadResponse<object>(ListData, TotalData, OrderDictionary);
        }

        public GarmentStockOpname ReadById(int id)
        {
            var data = dbSet.Where(p => p.Id == id)
                .Include(p => p.Items)
                    .ThenInclude(i => i.DOItem)
                .FirstOrDefault();
            return data;
        }

        public Stream Download(DateTimeOffset date, string unit, string storage, string storageName)
        {
            var data = dbSetDOItem.Where(i => i.UnitCode == unit && i.StorageCode == storage)
                .Select(i => new
                {
                    i.Id,
                    i.POSerialNumber,
                    i.RO,
                    i.ProductCode,
                    i.ProductName,
                    i.DesignColor,
                    BeforeQuantity = i.RemainingQuantity,
                    Quantity = i.RemainingQuantity
                })
                .ToList();

            if (data.Count > 0)
            {
                DataTable table = new DataTable();
                table.Columns.Add(new DataColumn() { ColumnName = "DOItemId", DataType = typeof(int) });
                table.Columns.Add(new DataColumn() { ColumnName = "PONo", DataType = typeof(string) });
                table.Columns.Add(new DataColumn() { ColumnName = "RONo", DataType = typeof(string) });
                table.Columns.Add(new DataColumn() { ColumnName = "Product Code", DataType = typeof(string) });
                table.Columns.Add(new DataColumn() { ColumnName = "Product Name", DataType = typeof(string) });
                table.Columns.Add(new DataColumn() { ColumnName = "Design Color", DataType = typeof(string) });
                table.Columns.Add(new DataColumn() { ColumnName = "Before Quantity", DataType = typeof(decimal) });
                table.Columns.Add(new DataColumn() { ColumnName = "Quantity", DataType = typeof(decimal) });

                foreach (var d in data)
                {
                    for (int i = 0; i < 100; i++)
                        table.Rows.Add(d.Id, d.POSerialNumber, d.RO, d.ProductCode, d.ProductName, d.DesignColor, d.BeforeQuantity, d.Quantity);
                }

                var excelPack = new ExcelPackage();
                var ws = excelPack.Workbook.Worksheets.Add("WriteTest");
                ws.Cells["A1"].Value = "Tanggal Stock Opname";
                ws.Cells["A2"].Value = "Unit";
                ws.Cells["A3"].Value = "Nama Gudang";
                ws.Cells["B1"].Value = date;
                ws.Cells["B2"].Value = unit;
                ws.Cells["B3"].Value = $"{storage} - {storageName}";
                ws.Cells["A5"].LoadFromDataTable(table, true);
                ws.Cells["H6:H" + data.Count + 6].Style.Locked = false;
                ws.Protection.IsProtected = true;
                ws.Cells[ws.Dimension.Address].AutoFitColumns();

                Stream stream = new MemoryStream();
                excelPack.SaveAs(stream);

                return stream;
            }
            else
            {
                throw new Exception("Tidak data yang cocok");
            }
        }

        public async Task<GarmentStockOpname> Upload(Stream stream)
        {
            ExcelPackage excelPackage = new ExcelPackage();
            excelPackage.Load(stream);

            var ws = excelPackage.Workbook.Worksheets[0];

            var storage = ((string)ws.Cells["B3"].Value).Split("-");

            var data = new GarmentStockOpname
            {
                Date = DateTimeOffset.Parse((string)ws.Cells["B1"].Value),
                UnitCode = (string)ws.Cells["B2"].Value,
                StorageCode = storage[0].Trim(),
                StorageName = storage[1].Trim(),
                Items = new List<GarmentStockOpnameItem>()
            };
            EntityExtension.FlagForCreate(data, identityService.Username, USER_AGENT);

            for (int row = 6; row <= ws.Dimension.End.Row; row++)
            {
                if (!string.IsNullOrWhiteSpace(ws.Cells[row, 1].Text))
                {
                    GarmentStockOpnameItem item = new GarmentStockOpnameItem
                    {
                        DOItemId = (int)(double)ws.Cells[row, 1].Value,
                        BeforeQuantity = (decimal)(double)ws.Cells[row, 7].Value,
                        Quantity = (decimal)(double)ws.Cells[row, 8].Value
                    };

                    if (item.BeforeQuantity != item.Quantity)
                    {
                        EntityExtension.FlagForCreate(item, identityService.Username, USER_AGENT);
                        data.Items.Add(item);

                        item.DOItem = dbSetDOItem.Where(doi => doi.Id == item.DOItemId).Single();
                        item.DOItem.RemainingQuantity = item.Quantity;
                        EntityExtension.FlagForUpdate(item.DOItem, identityService.Username, USER_AGENT);
                    }
                }
            }

            dbSet.Add(data);

            await dbContext.SaveChangesAsync();

            return data;
        }
    }
}