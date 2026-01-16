using Application.IServices;
using Data.Models;
using Data.ViewModels;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Application.Services
{
    public class ProductService : IProductService
    {
        private readonly DrugRepositoryDBContext _context;
        public ProductService(DrugRepositoryDBContext context)
        {
            _context = context;
        }
        public async Task<List<ProductView>> getAllProduct()
        {
            List<ProductView> result = new List<ProductView>();
            try
            {
                result = await _context.Set<ProductView>().FromSqlRaw("sp_GetAll_Product").ToListAsync();
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<List<ProductTypeView>> getAllProductType()
        {
            List<ProductTypeView> result = new List<ProductTypeView>();
            try
            {
                result = await _context.Set<ProductTypeView>().FromSqlRaw("sp_GetAll_ProductType").ToListAsync();
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}