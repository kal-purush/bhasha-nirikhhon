using ShoppingSpree.Data.Models;
using ShoppingSpree.Data.Services;
using ShoppingSpree.Data.ViewModels;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShoppingSpree.Data.ModelBuilder
{
    public class ProductsModelBuilder
    {
        private ProductDbContext _db;

        public ProductsModelBuilder(ProductDbContext db)
        {
            _db = db;
        }

        public async Task addAsync(ProductViewModel model)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            {
                await model.Image.InputStream.CopyToAsync(memoryStream);
                byte[] imagebyte = memoryStream.ToArray();

                _db.Products.Add(new ProductsModel()
                {
                    ProductName = model.ProductName,
                    ProductColour = model.ProductColour,
                    ProductPrice = model.ProductPrice,
                    Image = imagebyte
                });

                await _db.SaveChangesAsync();
            }
        }

        public ProductsModel GetById(int id)
        {
            return _db.Products.FirstOrDefault(x => x.ProductId == id);
        }
    }
}