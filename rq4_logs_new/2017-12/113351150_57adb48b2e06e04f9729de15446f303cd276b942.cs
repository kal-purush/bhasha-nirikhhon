using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProductCommon
{
    public interface IProductProvider
    {
        IEnumerable<Product> GetProducts();
        Product GetProduct(int productId);
        Product AddProduct(Product product);
        Product UpdateProduct(Product product);
        bool DeleteProduct(int productId);
    }
}