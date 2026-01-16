using System;
using System.Collections.Generic;
using System.Linq;
using Com.Bateeq.Service.Warehouse.Lib.Helpers;
using Com.Bateeq.Service.Warehouse.Lib.Interfaces.InventoryLoaderInterfaces;
using Com.Bateeq.Service.Warehouse.Lib.Models.InventoryModel;
using Com.Moonlay.NetCore.Lib;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;

namespace Com.Bateeq.Service.Warehouse.Lib.Facades
{
    public class InventoryLoaderFacade: IInventoryLoader
    {
        private string USER_AGENT = "Facade";

        private readonly WarehouseDbContext dbContext;
        private readonly DbSet<Inventory> dbSet;
        private readonly IServiceProvider serviceProvider;

        public InventoryLoaderFacade(IServiceProvider serviceProvider, WarehouseDbContext dbContext)
        {
            this.serviceProvider = serviceProvider;
            this.dbContext = dbContext;
            this.dbSet = dbContext.Set<Inventory>();
        }

        public Tuple<List<Inventory>, int, Dictionary<string, string>> Read(int Page = 1, int Size = 25, string Order = "{}", string Keyword = null, string Filter = "{}")
        {
            IQueryable<Inventory> Query = dbContext.Inventories.Where(m => m.IsDeleted == false);

            List<string> searchAttributes = new List<string>()
            {
                "ItemCode", "ItemName"
            };

            Query = QueryHelper<Inventory>.ConfigureSearch(Query, searchAttributes, Keyword);

            Dictionary<string, string> FilterDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(Filter);
            Query = QueryHelper<Inventory>.ConfigureFilter(Query, FilterDictionary);

            Dictionary<string, string> OrderDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(Order);
            Query = QueryHelper<Inventory>.ConfigureOrder(Query, OrderDictionary);

            Pageable<Inventory> pageable = new Pageable<Inventory>(Query, Page - 1, Size);
            List<Inventory> Data = pageable.Data.ToList();
            int TotalData = pageable.TotalCount;

            return Tuple.Create(Data, TotalData, OrderDictionary);
        }
    }
}