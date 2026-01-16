using ECommerceSite.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Threading.Tasks;

namespace ECommerceSite.Data
{
    public static class ProductDb
    {
        /// <summary>
        /// Returns the total count of products
        /// </summary>
        /// <param name="_context">Database context to use</param>
        /// <returns></returns>
        public static async Task<int> GetTotalProductsAsync(ProductContext _context)
        {
            return await (from product in _context.Products
                          select product).CountAsync();
        }

        /// <summary>
        /// Get a page of products
        /// </summary>
        /// <param name="_context">Database context to use</param>
        /// <param name="PageSize">The number of products per page</param>
        /// <param name="pageNum">Page of products to return</param>
        public static async Task<List<Product>>
            GetProductsAsync(ProductContext _context, int PageSize, int pageNum)
        {
            return await(from product in _context.Products
                        orderby product.Title ascending
                        select product)
                        .Skip(PageSize * (pageNum - 1)) //Skip must be before Take
                        .Take(pageNum)
                        .ToListAsync();
        }

        public static async Task<Product> AddProductAsync( ProductContext _context, Product product)
        {
            // Add to Db
            _context.Products.Add(product);
            await _context.SaveChangesAsync();
            return product;
        }
    }
}