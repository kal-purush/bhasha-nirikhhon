using eCommerce.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace eCommerce.Data
{
    public static class ProductDB
    {
        public static async Task<int> GetTotalProductsAsync(ProductContext _context)
        {
            return await _context.Products.CountAsync();
        }

        /// <summary>
        /// Gets a single product frome tha database by Id
        /// </summary>
        /// <param name="_context"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public static async Task<Product> GetProductByIdAsync(ProductContext _context, int id)
        {
            return await _context.Products
                .Where(prod => prod.ProductId == id)
                .SingleAsync();
        }

        /// <summary>
        /// Returns a page worth of products
        /// </summary>
        /// <param name="_context"> the database context </param>
        /// <param name="pageNumber"> The page to retrieve</param>
        /// <param name="perPage"> Number of products per page < /param>
        /// <returns></returns>
        public static async Task<List<Product>> GetProductsAsync(ProductContext _context, int pageNumber, int perPage)
        {
            return await _context
                .Products
                .OrderBy(p => p.Title)
                .Skip(perPage * Math.Max(pageNumber - 1, 0))
                .Take(perPage)
                .ToListAsync();
        }

        public static async Task EditProductAsync(ProductContext _context, Product p)
        {
            _context.Entry(p).State = EntityState.Modified;

            await _context.SaveChangesAsync();
        }

        public static async Task DeleteProductAsync(ProductContext _context, int id)
        {
            Product p = await GetProductByIdAsync(_context, id);

            _context.Entry(p).State = EntityState.Deleted;

            await _context.SaveChangesAsync();
        }

        public static async Task AddProductAsync(ProductContext _context, Product p)
        {
            _context.Products.Add(p);


            await _context.SaveChangesAsync();
        }
    }
}