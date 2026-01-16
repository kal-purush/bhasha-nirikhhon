using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MyLibrary.Models;


namespace MyLibrary.DataAccess
{
    public class DataAccess
    {
        private DataClasses1DataContext dbContext;

        public DataAccess(DataClasses1DataContext dbContext)
        {
            this.dbContext = dbContext;
        }

        public List<Product> GetAllProducts()
        {
            return dbContext.Products.Select(item => new Product
            {
                Product_Number = item.Product_Number,
                Price = item.Price,
                Units_On_Hand = item.Units_On_Hand,
                Description = item.Description
            }).ToList();
        }

        public List<Product> SearchByProductNumber(string productNumber)
        {
            return dbContext.Products.Where(item => item.Product_Number == productNumber).Select(item => new Product
            {
                Product_Number = item.Product_Number,
                Price = item.Price,
                Units_On_Hand = item.Units_On_Hand,
                Description = item.Description
            }).ToList();
        }

        public List<Product> SearchByDescription(string description)
        {
            return dbContext.Products.Where(item => item.Description.Contains(description)).Select(item => new Product
            {
                Product_Number = item.Product_Number,
                Price = item.Price,
                Units_On_Hand = item.Units_On_Hand,
                Description = item.Description
            }).ToList();
        }

        
    }
}
