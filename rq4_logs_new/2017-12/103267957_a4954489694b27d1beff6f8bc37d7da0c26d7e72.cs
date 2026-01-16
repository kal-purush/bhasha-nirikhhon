using DataBase.Context;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Business.Features
{
    public class GraphData : IGraphData
    {
        private readonly DataContext _dataContext;
        public GraphData(DataContext dataContext)
        {
            _dataContext = dataContext;
        }

        public Dictionary<DateTime, decimal> GetDaysList(string item, int storeNum)
        {
            using (_dataContext)
            {
                var dict = new Dictionary<DateTime, decimal>();
                var itemId = (from r in _dataContext.Products
                             where r.Name == item
                             select r.Id).First();

                var daysList = from r in _dataContext.Records
                               where r.StoreId == storeNum && r.ProductId == itemId
                               select r;

                foreach (var record in daysList)
                {
                    dict.Add(record.TimeStamp, record.Price);
                }

                return dict;

            }
        }

        public List<string> GetProducts(int storeNum)
        {
            using (_dataContext)
            {
                var productsList = _dataContext.Records
                    .Where(record => record.StoreId == storeNum)
                    .Join(_dataContext.Products,
                          record => record.ProductId,
                          product => product.Id,
                          (record, product) =>
                          new
                          {
                              product.Name
                          })
                    .Select(x => x.Name).ToList();

                return productsList;     
            }
        }

        public List<string> GetStores()
        {
            using (_dataContext)
            {
                var storesList = _dataContext.Stores
                    .Select(store => store.Name).ToList();
                return storesList;
            }
        }
    }
}