using Data.Data;
using Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Data.Repositories
{
    public class Repository<TEntity> where TEntity : BaseEntity
    {
        public readonly OnlineLibriaryDBContext Context;
        public Repository(OnlineLibriaryDBContext context)
        {
            Context = context;
        }
        public async Task<TEntity> GetByIdAsync(int id)
        {
            return await Context.Set<TEntity>().FirstOrDefaultAsync(x => x.Id.Equals(id));
        }

        public async Task<TEntity> UpdateAsync(TEntity entity)
        {
            TEntity entityToUpdate = await Context.Set<TEntity>().FirstAsync(x => x.Id.Equals(entity.Id));

            entityToUpdate = entity;

            return entity;
        }

        public async Task AddAsync(TEntity entity)
        {
            await Context.Set<TEntity>().AddAsync(entity);
        }
        public async Task<TEntity> DeleteByIdAsync(int id)
        {
            var entity = await GetByIdAsync(id);
            Context.Set<TEntity>().Remove(entity);
            return entity;
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync()
        {
            return (await Context.Set<TEntity>().ToListAsync()).OrderBy(x => x.Id);
        }
    }
}