using DataBase.Context;
using DataBase.Models;
using System.Collections.Generic;
using System.Linq;

namespace Business.Features
{
    public class Cheapest: ICheapest
    {
        private readonly DataContext _dataContext;
        decimal lowestSum = decimal.MaxValue;
        Store cheapestStore;

        public Cheapest(DataContext dataContext)
        {
            _dataContext = dataContext;
        }

        public Dictionary<string, decimal> GetCheapest(List<string> products)
        {
            using (_dataContext)
            {
                var stores = GetStores();
                foreach (var store in stores)
                {
                    decimal sum = 0;
                    foreach (var product in products)
                    {
                        var prod = _dataContext.Records
                                .Where(record => record.StoreId == store.Id)
                                .Where(record => record.Product.Name == product)
                                    .OrderByDescending(record => record.TimeStamp)
                                          .FirstOrDefault();
                        if(prod!= null)
                            sum+= prod.Price / prod.Quantity;
                    }
                    if (sum < lowestSum && sum != 0)
                    {
                        lowestSum = sum;
                        cheapestStore = store;
                    }
                }
            }
            var result = new Dictionary<string, decimal>();
            result.Add(cheapestStore.Name.ToString(), lowestSum);
           return result;
        }

        private List<Store> GetStores()
        {
            IQueryable<Store> temp = from store in _dataContext.Stores select store;
            return temp.ToList();
        }
    }
}