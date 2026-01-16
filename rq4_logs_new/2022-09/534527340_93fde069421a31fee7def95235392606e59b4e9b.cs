using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Battleships.Data;
using Battleships.Models;
using Microsoft.EntityFrameworkCore;

namespace Battleships.Repositories
{
    public class BaseRepository<TModel> : IRepository<TModel> where TModel : BaseModel
    {
        private readonly ApplicationDbContext _context;

        private DbSet<TModel> ItemSet => _context.Set<TModel>();

        public BaseRepository(ApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<TModel> GetById(Guid id)
        {
            return await ItemSet.FirstOrDefaultAsync(m => m.Id == id);
        }

        public async Task<List<TModel>> GetWhere(Expression<Func<TModel, bool>> filter)
        {
            return await ItemSet.Where(filter).ToListAsync();
        }

        public async Task<Guid> Create(TModel model)
        {
            await ItemSet.AddAsync(model);
            await SaveChanges();

            return model.Id;
        }

        public async Task Update(TModel model)
        {
            ItemSet.Update(model);
            await SaveChanges();
        }

        public async Task Delete(TModel model)
        {
            ItemSet.Remove(model);
            await SaveChanges();
        }

        public async Task<List<Guid>> CreateMany(List<TModel> models)
        {
            await ItemSet.AddRangeAsync(models);
            await SaveChanges();

            return models.Select(m => m.Id).ToList();
        }

        public async Task UpdateMany(List<TModel> models)
        {
            ItemSet.UpdateRange(models);
            await SaveChanges();
        }

        public async Task DeleteMany(List<TModel> models)
        {
            ItemSet.RemoveRange(models);
            await SaveChanges();
        }

        private async Task SaveChanges()
        {
            await _context.SaveChangesAsync();
        }
    }
}