using SNOW.SHOP.API.Data;
using SNOW.SHOP.API.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SNOW.SHOP.API.src.Data
{
    public class DbInitializer
    {
        public static void Initialize(SnowShopAPIDbContext context)
        {
            context.Database.EnsureCreated();

            // Look for any students.
            if (context.Products.Any())
            {
                return;   // DB has been seeded
            }

            var Products = new Product[]
            {
            new Product{Name="laptop",Description="laptop", MarketPrice =2.11M, StockPrice=3.11M, CreatedDate = DateTime.Now },
            new Product{Name="top",Description="top", MarketPrice =2.11M, StockPrice=3.11M, CreatedDate = DateTime.Now },
            new Product{Name="t-shirt",Description="t-shirt", MarketPrice =2.11M, StockPrice=3.11M, CreatedDate = DateTime.Now },
            new Product{Name="jeans",Description="jeans", MarketPrice =2.11M, StockPrice=3.11M, CreatedDate = DateTime.Now },
            };
            foreach (Product p in Products)
            {
                context.Products.Add(p);
            }
            context.SaveChanges();

            
        }

    }
}