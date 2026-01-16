using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WiredBrainCoffee.DataAccess.Model;

namespace WiredBrainCoffee.DataAccess
{
    public class CoffeeShopDataProvider
    {
        public IEnumerable<CoffeeShop> LoadCoffeeShops()
        {
            yield return new CoffeeShop { Location = "Frankfurt", BeansInStockInKg = 107 };
            yield return new CoffeeShop { Location = "Freiburg", BeansInStockInKg = 23 };
            yield return new CoffeeShop { Location = "Munich", BeansInStockInKg = 56 };

        }
    }
}