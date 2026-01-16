using System.Collections.Generic;
using System.Threading.Tasks;
using Green.Context;
using Green.Models;
using Microsoft.EntityFrameworkCore;

namespace Green.Services
{
    public class ProductsService : IProduct
    {
        private readonly Greencontext _greencontext;

        public ProductsService(Greencontext greencontext)
        {
            _greencontext = greencontext;

        }

        public Task<SResponse<List<Product>>> AddNewProduct(Product newProduct)
        {
            throw new System.NotImplementedException();
        }

        public Task<SResponse<List<Product>>> DeleteProduct(int id)
        {
            throw new System.NotImplementedException();
        }

        public async Task<SResponse<List<Product>>> GetAllProducts()
        {
            SResponse<List<Product>> serviceResponse = new SResponse<List<Product>>();
            List<Product> dbProduct = await _greencontext.Products.ToListAsync();

            serviceResponse.Data = dbProduct;
            return serviceResponse;
        }

        public Task<SResponse<Product>> GetProductById(int id)
        {
            throw new System.NotImplementedException();
        }

        public Task<SResponse<Product>> UpdateProduct(Product UpdateProduct, int id)
        {
            throw new System.NotImplementedException();
        }
    }
}