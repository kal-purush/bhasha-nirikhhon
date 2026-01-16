using Microsoft.EntityFrameworkCore;
using SPSS.Data;
using SPSS.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Repository.Repositories.BrandReposiotry
{
    public class BrandRepository(AppDbContext _context) : IBrandRepository
    {
        public async Task AddAsync(Brand p)
        {
            await _context.Brands.AddAsync(p);
            await _context.SaveChangesAsync();
        }

        public async Task DeleteAsync(int id)
        {
            var brand = await _context.Brands.FindAsync(id);
            brand.isDelete = true;
            await _context.SaveChangesAsync();
        }

        public async Task<IEnumerable<Brand>> GetAllAsync()
        {
            return await _context.Brands.ToListAsync();
        }

        public async Task<Brand> GetByIdAsync(int id)
        {
            return await _context.Brands.FindAsync(id);
        }

        public async Task<Brand> GetByNameAsync(string name)
        {
            return await _context.Brands.FirstOrDefaultAsync(b => b.BrandName == name);
        }

        public async Task UpdateAsync(Brand p)
        {
            _context.Brands.Update(p);
            await _context.SaveChangesAsync();
        }
    }
}