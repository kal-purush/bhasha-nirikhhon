using Microsoft.EntityFrameworkCore;
using System.Linq.Expressions;
using Task_Manager_Application.Mvc.Data;

namespace Task_Manager_Application.Mvc.Infrastructure.Repository
{
    public class BaseRepository<TEntity> : IBaseRepository<TEntity> where TEntity : class
    {
        private readonly TaskManagerDbContext _dbContext;
        private readonly DbSet<TEntity> _dbSet;

        public BaseRepository(TaskManagerDbContext dbContext)
        {
            _dbContext = dbContext;
            _dbSet = _dbContext.Set<TEntity>();
        }

        public async Task<TEntity> AddAsync(TEntity entity)
        {
            await _dbSet.AddAsync(entity);
            return entity;
        }

        public async Task DeleteAsync(Expression<Func<TEntity, bool>> expression)
        {
            var entity = _dbSet.Find(expression);
            if (entity != null)
            {
                _dbSet.Remove(entity);
            }
        }

        public async Task<List<TEntity>> GetAllAsync()
        {
            var entities = await _dbSet.ToListAsync();
            return entities;
        }

        public async Task<TEntity> GetAsync(Expression<Func<TEntity, bool>> expression)
        {
            var entity = await _dbSet.Where(expression).FirstOrDefaultAsync();
            return entity;
        }

        public async Task<List<TEntity>> ListAsync(Expression<Func<TEntity, bool>> expression)
        {
            var entities = await _dbSet.Where(expression).ToListAsync();
            return entities;
        }

        public async Task<TEntity> UpdateAsync(TEntity entity)
        {
            _dbSet.Update(entity);
            return entity;
        }
    }
}